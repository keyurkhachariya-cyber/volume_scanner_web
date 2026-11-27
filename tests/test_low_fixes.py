import importlib
import os
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

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
    pd_stub.MultiIndex = type("MultiIndex", (), {})
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

import baseline_cache as bc
import holidays as h
import scanner as s


class LowFixesTest(unittest.TestCase):

    def test_interval_switch_skips_auto_scan_only_when_target_cached(self):
        class SessionState(dict):
            def __getattr__(self, k):
                try:
                    return self[k]
                except KeyError as exc:
                    raise AttributeError(k) from exc

            def __setattr__(self, k, v):
                self[k] = v

        st_stub = MagicMock()
        st_stub.session_state = SessionState(
            scan_times_by_interval={"1m": datetime(2026, 11, 25, 10, 0, tzinfo=h.IST)},
            _last_filter_state=(1.5, 50000, False, False, 0.5, 8, False, None, (), "Ratio (High → Low)", None),
            _last_interval="1m",
        )
        st_stub.cache_resource = lambda fn: fn
        st_stub.checkbox.return_value = False
        st_stub.button.return_value = False
        st_stub.number_input.side_effect = [1.5, 50000]
        st_stub.slider.return_value = 8
        st_stub.sidebar.__enter__ = lambda *a: st_stub
        st_stub.sidebar.__exit__ = lambda *a: False

        # Switching to uncached "5m" should NOT suppress auto-scan
        st_stub.selectbox.side_effect = ["5m", "Ratio (High → Low)"]
        sys.modules.pop("app", None)
        with patch.dict(sys.modules, {"streamlit": st_stub}):
            importlib.import_module("app")
        self.assertFalse(st_stub.session_state._skip_auto_scan_once)

        # Switching back to cached "1m" SHOULD suppress auto-scan once
        st_stub.number_input.side_effect = [1.5, 50000]
        st_stub.selectbox.side_effect = ["1m", "Ratio (High → Low)"]
        sys.modules.pop("app", None)
        with patch.dict(sys.modules, {"streamlit": st_stub}):
            importlib.import_module("app")
        self.assertTrue(st_stub.session_state._skip_auto_scan_once)
        sys.modules.pop("app", None)

    def test_auto_scan_cadence_uses_scan_started_timestamp(self):
        class SessionState(dict):
            def __getattr__(self, k):
                return self[k]

            def __setattr__(self, k, v):
                self[k] = v

        st_stub = MagicMock()
        st_stub.session_state = SessionState()
        st_stub.cache_resource = lambda fn: fn
        st_stub.checkbox.return_value = False
        st_stub.button.return_value = False
        st_stub.selectbox.return_value = "1m"
        st_stub.number_input.side_effect = [1.5, 50000]
        st_stub.slider.return_value = 8
        st_stub.sidebar.__enter__ = lambda *a: st_stub
        st_stub.sidebar.__exit__ = lambda *a: False

        sys.modules.pop("app", None)
        try:
            with patch.dict(sys.modules, {"streamlit": st_stub}):
                app = importlib.import_module("app")
        finally:
            sys.modules.pop("app", None)

        now = datetime(2026, 11, 25, 10, 1, 0, tzinfo=h.IST)
        started = datetime(2026, 11, 25, 10, 0, 0, tzinfo=h.IST)
        completed = datetime(2026, 11, 25, 10, 0, 6, tzinfo=h.IST)

        self.assertTrue(
            app._should_auto_scan(
                now=now,
                last_scan_dt=completed,
                refresh_seconds=60,
                market_open=True,
                holidays_set=set(),
                skip_auto_scan=False,
                last_scan_started_dt=started,
            )
        )

    def test_get_or_load_store_populates_lru_once(self):
        from datetime import date

        prev_day = date(2026, 11, 23)
        key = (prev_day, "1m")
        with bc._CACHE_LOCK:
            bc._PREV_DAY_MEMORY_CACHE.clear()
            bc._NEG_CACHE.clear()

        fake_store = {
            "version": bc.BASELINE_FORMAT_VERSION,
            "prev_day": prev_day,
            "interval": "1m",
            "times_sec": [],
            "symbols": ["INFY.NS"],
            "symbol_to_idx": {"INFY.NS": 0},
            "cumvol": [],
            "prev_close": [],
            "prev_full_vol": [],
            "missing_symbols": set(),
        }
        with patch.object(bc, "_load_disk_baseline", return_value=fake_store) as mock_load:
            st1 = bc._get_or_load_store(key, prev_day, "1m")
            st2 = bc._get_or_load_store(key, prev_day, "1m")
            self.assertIs(st1, fake_store)
            self.assertIs(st2, fake_store)
            self.assertEqual(mock_load.call_count, 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
