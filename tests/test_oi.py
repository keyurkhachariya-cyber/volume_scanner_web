import json
import math
import os
import sys
import tempfile
import types
import unittest
from collections import deque
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

if "pytz" not in sys.modules:
    pytz_stub = types.ModuleType("pytz")
    IST_TZ = timezone(timedelta(hours=5, minutes=30), name="Asia/Kolkata")
    pytz_stub.timezone = lambda name: IST_TZ
    sys.modules["pytz"] = pytz_stub

if "numpy" not in sys.modules:
    class FakeArray(list):
        def __ge__(self, other):
            return FakeMask([x >= other for x in self])

        def __le__(self, other):
            return FakeMask([x <= other for x in self])

        def __lt__(self, other):
            return FakeMask([x < other for x in self])

        def __ne__(self, other):
            return FakeMask([x != other for x in self])

        def __getitem__(self, item):
            if isinstance(item, FakeMask):
                return FakeArray([x for x, keep in zip(self, item) if keep])
            return super().__getitem__(item)

    class FakeMask(list):
        def __and__(self, other):
            return FakeMask([a and b for a, b in zip(self, other)])

        def any(self):
            return any(self)

    np_stub = types.ModuleType("numpy")
    np_stub.int32 = int
    np_stub.int64 = int
    np_stub.float64 = float
    np_stub.nan = float("nan")
    np_stub.ndarray = object
    np_stub.iinfo = lambda dt: types.SimpleNamespace(min=-(1 << 63))
    np_stub.empty = lambda *a, **kw: FakeArray()
    np_stub.array = lambda vals, dtype=None: FakeArray(vals)
    np_stub.zeros = lambda shape, dtype=None: [
        [0.0] * shape[1] for _ in range(shape[0])
    ] if isinstance(shape, tuple) and len(shape) == 2 else [0.0] * int(shape)

    def _fake_asarray(vals, dtype=None):
        if dtype is int:
            for v in vals:
                if v is None:
                    raise TypeError("None not allowed in int array")
            return FakeArray([int(v) for v in vals])
        return FakeArray([float("nan") if v is None else float(v) for v in vals])

    np_stub.asarray = _fake_asarray
    np_stub.isnan = math.isnan
    np_stub.FakeArray = FakeArray
    sys.modules["numpy"] = np_stub

if "pandas" not in sys.modules:
    class FakeDF:
        def __init__(self, data=None):
            self.rows = list(data) if isinstance(data, list) else []
            self.empty = len(self.rows) == 0
            self.attrs = {}

        def sort_values(self, col, ascending=True):
            sorted_rows = sorted(
                self.rows,
                key=lambda r: (r.get(col) is None, r.get(col, 0)),
                reverse=not ascending,
            )
            out = FakeDF(sorted_rows)
            out.attrs = dict(self.attrs)
            return out

        def reset_index(self, drop=True):
            return self

    pd_stub = types.ModuleType("pandas")
    pd_stub.DataFrame = FakeDF
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

import baseline_cache
import oi
import scanner
import yahoo_client


def _make_payload(ts_str: str, curr_date: str, items):
    return {
        "data": items,
        "timestamp": ts_str,
        "currTradingDate": curr_date,
        "prevTradingDate": "25-Nov-2026",
    }


def _reset_oi_state():
    oi._CACHED_SNAPSHOT = None
    oi._LAST_FETCH_MONO = 0.0
    oi._CONSECUTIVE_FAILURES = 0
    oi._COOLDOWN_UNTIL_MONO = 0.0
    oi._LAST_ERROR = None
    oi._OI_SESSION = None
    oi._OI_SESSION_CREATED_MONO = 0.0


class OiModuleTest(unittest.TestCase):
    def setUp(self):
        _reset_oi_state()

    def tearDown(self):
        _reset_oi_state()

    def test_parse_oi_payload_parses_symbols_and_timestamps(self):
        payload = _make_payload(
            "26-Nov-2026 13:36:45",
            "26-Nov-2026",
            [
                {"symbol": "RELIANCE", "latestOI": 1000, "prevOI": 800, "changeInOI": 200, "avgInOI": 25.0},
                {"symbol": "M&M", "latestOI": 550, "prevOI": 500},
                {"symbol": "ZEROPREV", "latestOI": 120, "prevOI": 0, "changeInOI": 120},
                {"symbol": ""},
            ],
        )
        hist_date, epoch, ts_str, sym_map = oi._parse_oi_payload(payload)
        self.assertEqual(hist_date, date(2026, 11, 26))
        self.assertEqual(ts_str, "26-Nov-2026 13:36:45")
        self.assertGreater(epoch, 0.0)
        self.assertEqual(set(sym_map.keys()), {"RELIANCE", "M&M", "ZEROPREV"})
        self.assertEqual(sym_map["RELIANCE"]["oi"], 1000.0)
        self.assertEqual(sym_map["RELIANCE"]["oi_prev"], 800.0)
        self.assertEqual(sym_map["RELIANCE"]["oi_chg"], 200.0)
        self.assertAlmostEqual(sym_map["RELIANCE"]["oi_chg_pct"], 25.0)
        # Missing changeInOI/avgInOI falls back to arithmetic delta
        self.assertEqual(sym_map["M&M"]["oi_chg"], 50.0)
        self.assertAlmostEqual(sym_map["M&M"]["oi_chg_pct"], 10.0)
        self.assertIsNone(sym_map["ZEROPREV"]["oi_chg_pct"])

    def test_rate_limit_floor_and_closed_market_ttl(self):
        payload = _make_payload(
            "26-Nov-2026 13:30:00",
            "26-Nov-2026",
            [{"symbol": "TCS", "latestOI": 200, "prevOI": 180, "changeInOI": 20, "avgInOI": 11.11}],
        )
        now = [1000.0]
        market_open = [True]

        with (
            patch.object(oi.pytime, "monotonic", side_effect=lambda: now[0]),
            patch.object(oi, "is_market_open", side_effect=lambda *a, **kw: market_open[0]),
            patch.object(oi, "_fetch_nse_oi_raw", return_value=payload) as mock_fetch,
        ):
            r1 = oi.get_oi_snapshot()
            self.assertFalse(r1["from_cache"])
            self.assertEqual(mock_fetch.call_count, 1)
            self.assertIn("TCS", r1["by_symbol"])

            # Within 45s live floor -> served from cache without hitting NSE
            now[0] = 1020.0
            r2 = oi.get_oi_snapshot()
            self.assertTrue(r2["from_cache"])
            self.assertEqual(mock_fetch.call_count, 1)

            # Closed market -> 300s TTL even past 45s
            market_open[0] = False
            now[0] = 1120.0
            r3 = oi.get_oi_snapshot()
            self.assertTrue(r3["from_cache"])
            self.assertEqual(mock_fetch.call_count, 1)

    def test_401_403_rewarm_single_retry(self):
        payload_ok = _make_payload(
            "26-Nov-2026 13:31:00",
            "26-Nov-2026",
            [{"symbol": "INFY", "latestOI": 300, "prevOI": 250}],
        )
        sessions_created = []
        get_calls = []

        class FakeResp:
            def __init__(self, status_code, data=None):
                self.status_code = status_code
                self._data = data

            def json(self):
                return self._data

        class FakeSession:
            def __init__(self, *args, **kwargs):
                self.idx = len(sessions_created)
                sessions_created.append(self)

            def get(self, url, **kwargs):
                get_calls.append((self.idx, url))
                if url == oi._NSE_OI_WARMUP_URL:
                    return FakeResp(200, {})
                if self.idx == 0:
                    return FakeResp(403, {})
                return FakeResp(200, payload_ok)

            def close(self):
                pass

        cffi_mod = types.ModuleType("curl_cffi")
        cffi_req_mod = types.ModuleType("curl_cffi.requests")
        cffi_req_mod.Session = FakeSession
        cffi_mod.requests = cffi_req_mod

        with (
            patch.dict(sys.modules, {"curl_cffi": cffi_mod, "curl_cffi.requests": cffi_req_mod}),
            patch.object(oi.pytime, "sleep") as mock_sleep,
        ):
            data = oi._fetch_nse_oi_raw(timeout=5)

        self.assertEqual(data["timestamp"], "26-Nov-2026 13:31:00")
        self.assertEqual(len(sessions_created), 2)
        self.assertEqual(
            get_calls,
            [
                (0, oi._NSE_OI_WARMUP_URL),
                (0, oi._NSE_OI_API_URL),
                (1, oi._NSE_OI_WARMUP_URL),
                (1, oi._NSE_OI_API_URL),
            ],
        )
        mock_sleep.assert_called_once()

    def test_429_backoff_and_cached_fallback(self):
        payload_ok = _make_payload(
            "26-Nov-2026 13:30:00",
            "26-Nov-2026",
            [{"symbol": "RELIANCE", "latestOI": 1000, "prevOI": 900}],
        )
        now = [2000.0]

        with (
            patch.object(oi.pytime, "monotonic", side_effect=lambda: now[0]),
            patch.object(oi, "is_market_open", return_value=True),
            patch.object(oi, "_fetch_nse_oi_raw", side_effect=[payload_ok, RuntimeError("NSE HTTP 429"), RuntimeError("NSE HTTP 429")]) as mock_fetch,
        ):
            r1 = oi.get_oi_snapshot()
            self.assertIsNone(r1["error"])
            self.assertEqual(r1["by_symbol"]["RELIANCE"]["oi"], 1000.0)

            # First failure past 45s floor -> 60s cooldown, still serves cached RELIANCE
            now[0] = 2060.0
            r2 = oi.get_oi_snapshot()
            self.assertIn("429", r2["error"])
            self.assertEqual(r2["cooldown_seconds"], 60.0)
            self.assertTrue(r2["from_cache"])
            self.assertEqual(r2["by_symbol"]["RELIANCE"]["oi"], 1000.0)
            self.assertEqual(mock_fetch.call_count, 2)

            # During cooldown -> no HTTP attempt
            now[0] = 2090.0
            r_mid = oi.get_oi_snapshot(force_refresh=True)
            self.assertEqual(mock_fetch.call_count, 2)
            self.assertGreater(r_mid["cooldown_seconds"], 0.0)

            # After 60s cooldown -> second failure doubles backoff to 120s
            now[0] = 2130.0
            r3 = oi.get_oi_snapshot()
            self.assertEqual(mock_fetch.call_count, 3)
            self.assertEqual(r3["cooldown_seconds"], 120.0)

    def test_cached_snapshot_updates_after_interval_without_5m_fields(self):
        p0 = _make_payload(
            "26-Nov-2026 13:30:00",
            "26-Nov-2026",
            [{"symbol": "RELIANCE", "latestOI": 1000, "prevOI": 800}],
        )
        p1 = _make_payload(
            "26-Nov-2026 13:31:00",
            "26-Nov-2026",
            [{"symbol": "RELIANCE", "latestOI": 1250, "prevOI": 800}],
        )
        now = [1000.0]

        with (
            patch.object(oi.pytime, "monotonic", side_effect=lambda: now[0]),
            patch.object(oi, "is_market_open", return_value=True),
            patch.object(oi, "_fetch_nse_oi_raw", side_effect=[p0, p1]),
        ):
            r0 = oi.get_oi_snapshot()
            self.assertEqual(r0["by_symbol"]["RELIANCE"]["oi"], 1000.0)
            self.assertEqual(set(r0["by_symbol"]["RELIANCE"].keys()), {"oi", "oi_prev", "oi_chg", "oi_chg_pct"})

            now[0] = 1050.0
            r1 = oi.get_oi_snapshot()
            self.assertFalse(r1["from_cache"])
            self.assertEqual(r1["nse_timestamp"], "26-Nov-2026 13:31:00")
            self.assertEqual(r1["by_symbol"]["RELIANCE"]["oi"], 1250.0)
            self.assertEqual(r1["by_symbol"]["RELIANCE"]["oi_chg"], 450.0)

    def test_scanner_run_scan_include_oi_merges_columns_and_attrs(self):
        from bisect import bisect_right

        IST = timezone(timedelta(hours=5, minutes=30), name="Asia/Kolkata")
        now_ist = datetime(2026, 11, 26, 13, 36, 0, tzinfo=IST)
        prev_day = date(2026, 11, 25)
        cache_key = (prev_day, "1m")

        class FakeMat:
            def __init__(self, rows):
                self._rows = rows

            def __getitem__(self, idx):
                r, c = idx
                return self._rows[r][c]

        fake_store = {
            "version": scanner.BASELINE_FORMAT_VERSION,
            "prev_day": prev_day,
            "interval": "1m",
            "times_sec": [9 * 3600 + 15 * 60, 13 * 3600 + 35 * 60],
            "symbols": ["RELIANCE.NS", "TCS.NS"],
            "symbol_to_idx": {"RELIANCE.NS": 0, "TCS.NS": 1},
            "cumvol": FakeMat([[5000.0, 3000.0], [20000.0, 10000.0]]),
            "prev_close": [100.0, 200.0],
            "prev_full_vol": [50000.0, 40000.0],
            "missing_symbols": set(),
        }
        fake_bundles = {
            "RELIANCE.NS": {
                "ts_sec": [1],
                "open": [102.0],
                "high": [106.0],
                "low": [101.0],
                "close": [105.0],
                "volume": [30000.0],
                "summary": (30000.0, 105.0, 102.0, 106.0, 101.0, time(13, 35)),
            },
            "TCS.NS": {
                "ts_sec": [1],
                "open": [201.0],
                "high": [205.0],
                "low": [200.0],
                "close": [204.0],
                "volume": [15000.0],
                "summary": (15000.0, 204.0, 201.0, 205.0, 200.0, time(13, 35)),
            },
        }
        fake_oi = {
            "by_symbol": {
                "RELIANCE": {
                    "oi": 1250.0,
                    "oi_prev": 1000.0,
                    "oi_chg": 250.0,
                    "oi_chg_pct": 25.0,
                },
                "TCS": {
                    "oi": 500.0,
                    "oi_prev": 500.0,
                    "oi_chg": 0.0,
                    "oi_chg_pct": 0.0,
                },
            },
            "nse_timestamp": "26-Nov-2026 13:36:45",
            "from_cache": False,
            "cooldown_seconds": 0.0,
            "error": None,
        }

        class LocalFakeDF:
            def __init__(self, data=None):
                self.rows = list(data) if isinstance(data, list) else []
                self.empty = len(self.rows) == 0
                self.attrs = {}

            def sort_values(self, col, ascending=True):
                sorted_rows = sorted(
                    self.rows,
                    key=lambda r: (r.get(col) is None, r.get(col, 0)),
                    reverse=not ascending,
                )
                out = LocalFakeDF(sorted_rows)
                out.attrs = dict(self.attrs)
                return out

            def reset_index(self, drop=True):
                return self

        def _fake_summary(ts_sec, open_a, high_a, low_a, close_a, vol_a, *args):
            if float(vol_a[0]) == 30000.0:
                return fake_bundles["RELIANCE.NS"]["summary"]
            return fake_bundles["TCS.NS"]["summary"]

        with baseline_cache._CACHE_LOCK:
            orig_mem = dict(baseline_cache._PREV_DAY_MEMORY_CACHE)
            baseline_cache._PREV_DAY_MEMORY_CACHE.clear()
            baseline_cache._PREV_DAY_MEMORY_CACHE[cache_key] = fake_store

        try:
            with (
                patch.object(scanner, "datetime", wraps=datetime) as mock_dt,
                patch.object(scanner, "get_previous_trading_day", return_value=prev_day),
                patch.object(baseline_cache, "_prune_old_cache"),
                patch.object(yahoo_client, "_fetch_market_data", return_value=(fake_bundles, set())),
                patch.object(scanner, "_summarize_today_arrays", side_effect=_fake_summary),
                patch.object(scanner.np, "searchsorted", side_effect=lambda arr, val, side="right": bisect_right(arr, val), create=True),
                patch.object(scanner, "get_oi_snapshot", return_value=fake_oi),
                patch.object(scanner.pd, "DataFrame", LocalFakeDF),
            ):
                mock_dt.now.return_value = now_ist
                df = scanner.run_scan(
                    ["RELIANCE.NS", "TCS.NS"],
                    min_ratio=0.0,
                    min_volume=0,
                    interval="1m",
                    prev_day=prev_day,
                    include_oi=True,
                )
        finally:
            with baseline_cache._CACHE_LOCK:
                baseline_cache._PREV_DAY_MEMORY_CACHE.clear()
                baseline_cache._PREV_DAY_MEMORY_CACHE.update(orig_mem)

        self.assertFalse(df.empty)
        by_sym = {r["Symbol"]: r for r in df.rows}
        self.assertEqual(by_sym["RELIANCE"]["OI"], 1250)
        self.assertEqual(by_sym["RELIANCE"]["OI_Prev"], 1000)
        self.assertEqual(by_sym["RELIANCE"]["OI_Chg"], 250)
        self.assertEqual(by_sym["RELIANCE"]["OI_Chg_Pct"], 25.0)
        self.assertEqual(by_sym["RELIANCE"]["OI_Buildup"], "Long Buildup")
        self.assertEqual(by_sym["RELIANCE"]["Signal"], oi.SIGNAL_BULLISH)
        self.assertNotIn("OI_5m_Chg", by_sym["RELIANCE"])
        self.assertNotIn("OI_5m_Chg_Pct", by_sym["RELIANCE"])
        self.assertEqual(by_sym["TCS"]["OI_Buildup"], "Neutral")
        self.assertEqual(by_sym["TCS"]["Signal"], oi.SIGNAL_NEUTRAL)
        self.assertEqual(df.attrs["oi_meta"]["nse_timestamp"], "26-Nov-2026 13:36:45")
        self.assertNotIn("by_symbol", df.attrs["oi_meta"])

    def test_classify_trade_signal_boundaries(self):
        self.assertEqual(oi.classify_trade_signal(1.01, 10.01, 1.51), oi.SIGNAL_BULLISH)
        self.assertEqual(oi.classify_trade_signal(1.01, 10.01, -1.51), oi.SIGNAL_BEARISH)
        # Strict boundary checks: <= thresholds stay neutral
        self.assertEqual(oi.classify_trade_signal(1.0, 15.0, 2.0), oi.SIGNAL_NEUTRAL)
        self.assertEqual(oi.classify_trade_signal(2.0, 10.0, 2.0), oi.SIGNAL_NEUTRAL)
        self.assertEqual(oi.classify_trade_signal(2.0, 15.0, 1.5), oi.SIGNAL_NEUTRAL)
        self.assertEqual(oi.classify_trade_signal(2.0, 15.0, -1.5), oi.SIGNAL_NEUTRAL)
        # Negative OI (Long Unwinding) stays neutral under > +10% rule
        self.assertEqual(oi.classify_trade_signal(2.0, -25.0, -3.0), oi.SIGNAL_NEUTRAL)
        self.assertEqual(oi.classify_trade_signal(None, 15.0, 2.0), oi.SIGNAL_NEUTRAL)
        self.assertEqual(oi.classify_trade_signal(2.0, float("nan"), 2.0), oi.SIGNAL_NEUTRAL)

    def test_classify_oi_buildup_regimes(self):
        self.assertEqual(oi.classify_oi_buildup(1.5, 100), "Long Buildup")
        self.assertEqual(oi.classify_oi_buildup(-1.5, 100), "Short Buildup")
        self.assertEqual(oi.classify_oi_buildup(1.5, -100), "Short Covering")
        self.assertEqual(oi.classify_oi_buildup(-1.5, -100), "Long Unwinding")
        self.assertEqual(oi.classify_oi_buildup(0.0, 100), "Neutral")
        self.assertEqual(oi.classify_oi_buildup(1.5, 0), "Neutral")
        self.assertIsNone(oi.classify_oi_buildup(None, 100))
        self.assertIsNone(oi.classify_oi_buildup(1.5, None))
        self.assertIsNone(oi.classify_oi_buildup(float("nan"), 100))

    def test_decode_json_response_uses_orjson_when_available(self):
        class RespBytes:
            content = b'{"ok":true}'

            def json(self):
                raise AssertionError("fallback json() should not be called when orjson handles bytes")

        fake_orjson = types.SimpleNamespace(loads=lambda b: {"ok": b == b'{"ok":true}'})
        with patch.object(oi, "_orjson", fake_orjson):
            self.assertEqual(oi._decode_json_response(RespBytes()), {"ok": True})


if __name__ == "__main__":
    unittest.main()
