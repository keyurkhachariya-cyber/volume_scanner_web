import importlib
import json
import os
import sys
import tempfile
import threading
import types
import unittest
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Minimal stubs for third-party libs unavailable in sandbox Python
if "pytz" not in sys.modules:
    pytz_stub = types.ModuleType("pytz")
    IST_TZ = timezone(timedelta(hours=5, minutes=30), name="Asia/Kolkata")
    pytz_stub.timezone = lambda name: IST_TZ
    sys.modules["pytz"] = pytz_stub

if "numpy" not in sys.modules:
    np_stub = types.ModuleType("numpy")
    np_stub.int32 = int
    np_stub.int64 = int
    np_stub.float64 = float
    np_stub.ndarray = object
    np_stub.iinfo = lambda dt: types.SimpleNamespace(min=-(1 << 63))
    np_stub.empty = lambda *a, **kw: []
    sys.modules["numpy"] = np_stub

if "pandas" not in sys.modules:
    pd_stub = types.ModuleType("pandas")
    pd_stub.DataFrame = type("DataFrame", (), {"empty": True})
    pd_stub.DatetimeIndex = object
    sys.modules["pandas"] = pd_stub

if "yfinance" not in sys.modules:
    yf_stub = types.ModuleType("yfinance")
    yf_data_stub = types.ModuleType("yfinance.data")
    yf_data_stub.YfData = MagicMock()
    yf_multi_stub = types.ModuleType("yfinance.multi")
    yf_multi_stub.new_session = lambda: object()
    yf_stub.data = yf_data_stub
    yf_stub.multi = yf_multi_stub
    sys.modules["yfinance"] = yf_stub
    sys.modules["yfinance.data"] = yf_data_stub
    sys.modules["yfinance.multi"] = yf_multi_stub

import core.baseline_cache as bc
import core.holidays as h
import core.universe as u
import providers.yahoo.client as yc


class FakeCurlSession:
    def __init__(self, payload_by_url):
        self.payload_by_url = payload_by_url
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.closed = True

    def get(self, url, timeout=10):
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json.return_value = self.payload_by_url.get(url, {})
        return resp


class MediumFixesTest(unittest.TestCase):
    def test_nse_fetchers_close_curl_sessions(self):
        created = []

        def make_session(impersonate="chrome"):
            sess = FakeCurlSession(
                {
                    "https://www.nseindia.com/api/holiday-master?type=trading": {
                        "FO": [{"tradingDate": "26-Jan-2026"}]
                    },
                    "https://www.nseindia.com/api/underlying-information": {
                        "data": {"UnderlyingList": [{"symbol": "RELIANCE"}]}
                    },
                }
            )
            created.append(sess)
            return sess

        curl_mod = types.ModuleType("curl_cffi")
        curl_req = types.ModuleType("curl_cffi.requests")
        curl_req.Session = make_session
        curl_mod.requests = curl_req

        with patch.dict(sys.modules, {"curl_cffi": curl_mod, "curl_cffi.requests": curl_req}):
            hols = h.fetch_nse_holidays()
            syms = u.fetch_nse_fno_universe()

        self.assertEqual(hols, {date(2026, 1, 26)})
        self.assertEqual(syms, ["RELIANCE.NS"])
        self.assertEqual(len(created), 2)
        self.assertTrue(all(sess.closed for sess in created))

    def test_universe_force_refresh_failure_falls_back_to_disk_cache(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_path = Path(tmpdir) / "fno_universe.json"
            cache_path.write_text(
                json.dumps({"date": "2026-11-24", "symbols": ["INFY.NS", "TCS.NS"]}),
                encoding="utf-8",
            )
            with (
                patch.object(u, "CACHE_FILE", cache_path),
                patch.object(u, "fetch_nse_fno_universe", side_effect=RuntimeError("blocked")),
            ):
                syms, src = u.get_fno_universe(force_refresh=True)
                self.assertEqual(syms, ["INFY.NS", "TCS.NS"])
                self.assertIn("refresh failed", src)

    def test_borrow_executor_defers_shutdown_until_idle(self):
        with yc._EXECUTOR_LOCK:
            yc._EXECUTORS.clear()

        with yc._borrow_executor(4) as ex4:
            # Force eviction of pool 4 by borrowing 4 more distinct sizes (5, 6, 7, 8)
            for w in (5, 6, 7, 8):
                with yc._borrow_executor(w):
                    pass
            # Evicted pool 4 must still accept work while actively borrowed!
            fut = ex4.submit(lambda x: x + 1, 41)
            self.assertEqual(fut.result(timeout=2), 42)

        # Once exited, retired pool 4 is shut down
        with self.assertRaises(RuntimeError):
            ex4.submit(lambda: 1)

        with yc._EXECUTOR_LOCK:
            for entry in yc._EXECUTORS.values():
                entry["executor"].shutdown(wait=False)
            yc._EXECUTORS.clear()

    def test_thread_local_sessions_are_isolated_per_worker(self):
        yc._YF_PRIMING_DONE = False
        if hasattr(yc._THREAD_LOCAL, "yf_session"):
            delattr(yc._THREAD_LOCAL, "yf_session")

        sessions = []

        def grab():
            sess = yc._get_yf_session()
            sessions.append(sess)
            self.assertIs(yc._get_yf_session(), sess)

        with (
            patch.object(yc, "YfData", MagicMock()),
            patch.object(yc, "_create_http_session", side_effect=lambda: object()),
        ):
            t1 = threading.Thread(target=grab)
            t2 = threading.Thread(target=grab)
            t1.start()
            t2.start()
            t1.join()
            t2.join()

        self.assertEqual(len(sessions), 2)
        self.assertIsNot(sessions[0], sessions[1])

    def test_atomic_save_and_commit_baseline_updates(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_cache = Path(tmpdir)
            prev_day = date(2026, 11, 23)
            cache_key = (prev_day, "1m")

            def fake_pack(new_entries, pday, interval, existing=None):
                merged = set(existing["symbols"]) if existing else set()
                merged.update(new_entries.keys())
                syms = sorted(merged)
                return {
                    "version": bc.BASELINE_FORMAT_VERSION,
                    "prev_day": pday,
                    "interval": interval,
                    "times_sec": [33300],
                    "symbols": syms,
                    "symbol_to_idx": {sym: i for i, sym in enumerate(syms)},
                    "cumvol": [[100.0] * len(syms)],
                    "prev_close": [10.0] * len(syms),
                    "prev_full_vol": [100.0] * len(syms),
                    "missing_symbols": set(),
                }

            with bc._CACHE_LOCK:
                bc._PREV_DAY_MEMORY_CACHE.clear()
                bc._NEG_CACHE.clear()

            with (
                patch.object(bc, "CACHE_DIR", tmp_cache),
                patch.object(bc, "_pack_baseline_store", side_effect=fake_pack),
            ):
                s1 = bc._commit_baseline_updates(cache_key, prev_day, "1m", {"INFY.NS": {"x": 1}}, ["FAIL.NS"])
                s2 = bc._commit_baseline_updates(cache_key, prev_day, "1m", {"TCS.NS": {"x": 1}}, [])
                self.assertEqual(s1["symbols"], ["INFY.NS"])
                self.assertEqual(s2["symbols"], ["INFY.NS", "TCS.NS"])
                self.assertEqual(list(tmp_cache.glob(".tmp_*")), [])
                loaded = bc._load_disk_baseline(prev_day, "1m")
                self.assertEqual(loaded["symbols"], ["INFY.NS", "TCS.NS"])

    def test_should_auto_scan_post_close_retry_cap(self):
        class SessionState(dict):
            def __getattr__(self, k):
                try:
                    return self[k]
                except KeyError as exc:
                    raise AttributeError(k) from exc

            def __setattr__(self, k, v):
                self[k] = v

        st_stub = MagicMock()
        st_stub.session_state = SessionState()
        st_stub.cache_resource = lambda fn: fn
        st_stub.checkbox.return_value = False
        st_stub.button.return_value = False
        st_stub.selectbox.return_value = "1m"
        st_stub.number_input.return_value = 1.5
        st_stub.slider.return_value = 8
        st_stub.sidebar.__enter__ = lambda *a: st_stub
        st_stub.sidebar.__exit__ = lambda *a: False

        sys.modules.pop("app", None)
        try:
            with patch.dict(sys.modules, {"streamlit": st_stub}):
                app = importlib.import_module("app")
        finally:
            sys.modules.pop("app", None)

        tz = h.IST
        now_post = datetime(2026, 11, 25, 15, 31, 0, tzinfo=tz)
        last_open = datetime(2026, 11, 25, 15, 29, 0, tzinfo=tz)
        last_post = datetime(2026, 11, 25, 15, 30, 30, tzinfo=tz)

        self.assertTrue(app._should_auto_scan(now_post, last_open, 60, False, set(), False, post_close_attempts=0))
        self.assertTrue(app._should_auto_scan(now_post, last_open, 60, False, set(), False, post_close_attempts=1))
        self.assertFalse(app._should_auto_scan(now_post, last_open, 60, False, set(), False, post_close_attempts=2))
        self.assertFalse(app._should_auto_scan(now_post, last_post, 60, False, set(), False, post_close_attempts=0))


if __name__ == "__main__":
    unittest.main(verbosity=2)
