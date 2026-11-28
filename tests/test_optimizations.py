import asyncio
import math
import os
import sys
import types
import unittest
from datetime import date, datetime, time, timedelta, timezone
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
        empty = True

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

import core.baseline_cache as bc
import core.holidays as h
import core.scanner as s
import core.utils as u
import providers.yahoo.client as yc


class FakeIndex:
    def __init__(self, values_ns, tz="Asia/Kolkata", has_as_unit=True):
        self.tz = tz
        self.asi8 = list(values_ns)
        if has_as_unit:
            self.as_unit = lambda unit: types.SimpleNamespace(
                asi8=s.np.asarray([v // 1_000_000_000 if v != -(1 << 63) else v for v in values_ns], dtype=s.np.int64)
                if unit == "s"
                else values_ns
            )

    def tz_localize(self, tz):
        return FakeIndex(self.asi8, tz=tz, has_as_unit=hasattr(self, "as_unit"))


class FakeAsyncResponse:
    def __init__(self, status_code, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        return self._payload


class OptimizationFixesTest(unittest.TestCase):
    def test_ist_epoch_sec_and_index_epoch_seconds(self):
        expected = int(datetime(2026, 11, 25, 3, 45, tzinfo=timezone.utc).timestamp())
        self.assertEqual(u.ist_epoch_sec(date(2026, 11, 25), time(9, 15)), expected)

        idx_ns = FakeIndex([expected * 1_000_000_000, -(1 << 63)], tz=None)
        secs = yc._index_epoch_seconds(idx_ns)
        self.assertEqual(list(secs), [expected, -(1 << 63)])

    def test_parse_yahoo_chart_payload_variants(self):
        # Valid bars with None gap
        payload = {
            "chart": {
                "result": [
                    {
                        "timestamp": [1000, 1060],
                        "indicators": {
                            "quote": [
                                {
                                    "open": [100.0, None],
                                    "high": [102.0, 103.0],
                                    "low": [99.5, 100.0],
                                    "close": [101.0, 102.5],
                                    "volume": [5000, None],
                                }
                            ]
                        },
                    }
                ],
                "error": None,
            }
        }
        bundle, ok = yc._parse_yahoo_chart_payload(payload)
        self.assertTrue(ok)
        self.assertEqual(list(bundle["ts_sec"]), [1000, 1060])
        self.assertTrue(math.isnan(bundle["open"][1]))
        self.assertTrue(math.isnan(bundle["volume"][1]))

        # Valid bars with None timestamp fallback
        none_ts_payload = {
            "chart": {
                "result": [
                    {
                        "timestamp": [1000, None],
                        "indicators": {"quote": [{"close": [101.0, 102.0], "volume": [5000, 10]}]},
                    }
                ],
                "error": None,
            }
        }
        bundle_nt, ok_nt = yc._parse_yahoo_chart_payload(none_ts_payload)
        self.assertTrue(ok_nt)
        self.assertEqual(list(bundle_nt["ts_sec"]), [1000, -(1 << 63)])

        # Pre-market empty bars -> ok=True, bundle=None
        empty_bars = {"chart": {"result": [{"meta": {}}], "error": None}}
        bundle, ok = yc._parse_yahoo_chart_payload(empty_bars)
        self.assertIsNone(bundle)
        self.assertTrue(ok)

        # Not Found -> ok=True, bundle=None
        not_found = {"chart": {"result": None, "error": {"code": "Not Found"}}}
        bundle, ok = yc._parse_yahoo_chart_payload(not_found)
        self.assertIsNone(bundle)
        self.assertTrue(ok)

        # Transient chart error -> ok=False
        transient = {"chart": {"result": None, "error": {"code": "Internal Server Error"}}}
        bundle, ok = yc._parse_yahoo_chart_payload(transient)
        self.assertIsNone(bundle)
        self.assertFalse(ok)

    def test_fetch_charts_async_crumb_retry_and_fallback(self):
        calls = []
        valid_payload = {
            "chart": {
                "result": [
                    {
                        "timestamp": [1000],
                        "indicators": {
                            "quote": [
                                {"open": [10.0], "high": [11.0], "low": [9.0], "close": [10.5], "volume": [100]}
                            ]
                        },
                    }
                ],
                "error": None,
            }
        }

        class FakeAsyncSession:
            def __init__(self, impersonate="chrome"):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def get(self, url, params=None, timeout=10):
                calls.append((url, params))
                if "fc.yahoo.com" in url:
                    return FakeAsyncResponse(200)
                if "getcrumb" in url:
                    return FakeAsyncResponse(200, text="CRUMB123")
                if "M%26M.NS" in url:
                    if not (params and params.get("crumb") == "CRUMB123"):
                        return FakeAsyncResponse(401)
                    return FakeAsyncResponse(200, payload=valid_payload)
                if "RETRYOK.NS" in url:
                    retry_seen = [u for u, _ in calls if "RETRYOK.NS" in u]
                    if len(retry_seen) == 1:
                        return FakeAsyncResponse(429)
                    return FakeAsyncResponse(200, payload=valid_payload)
                if "EMPTY.NS" in url:
                    return FakeAsyncResponse(200, payload={"chart": {"result": [{}], "error": None}})
                return FakeAsyncResponse(503)

        curl_mod = types.ModuleType("curl_cffi")
        curl_req = types.ModuleType("curl_cffi.requests")
        curl_req.AsyncSession = FakeAsyncSession
        curl_mod.requests = curl_req

        class DummyAsyncCtx:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

        async def dummy_gather(*coros):
            return [await c for c in coros]

        async def dummy_sleep(_secs):
            return None

        def run_coro_no_loop(coro):
            try:
                while True:
                    coro.send(None)
            except StopIteration as exc:
                return exc.value

        with (
            patch.dict(sys.modules, {"curl_cffi": curl_mod, "curl_cffi.requests": curl_req}),
            patch.object(asyncio, "Lock", return_value=DummyAsyncCtx()),
            patch.object(asyncio, "Semaphore", side_effect=lambda *a: DummyAsyncCtx()),
            patch.object(asyncio, "gather", side_effect=dummy_gather),
            patch.object(asyncio, "sleep", side_effect=dummy_sleep),
        ):
            results, failed = run_coro_no_loop(
                yc._fetch_charts_async(
                    tasks=[("M&M.NS", "1d"), ("RETRYOK.NS", "1d"), ("EMPTY.NS", "1d"), ("FAIL.NS", "1d")],
                    interval="1m",
                    max_concurrency=8,
                )
            )
        self.assertIn("M&M.NS", results)
        self.assertIn("RETRYOK.NS", results)
        self.assertNotIn("EMPTY.NS", results)
        self.assertEqual(failed, {"FAIL.NS"})
        # Check alternating wave-1 hosts and opposite-host retry
        m_wave1 = [url for url, _ in calls if "M%26M.NS" in url]
        self.assertTrue("query1" in m_wave1[0])
        retry_urls = [url for url, _ in calls if "RETRYOK.NS" in url]
        self.assertEqual(len(retry_urls), 2)
        self.assertNotEqual(retry_urls[0].split("/")[2], retry_urls[1].split("/")[2])

    def test_fetch_market_data_falls_back_to_yfinance_for_failed_symbols(self):
        fb_bundle = {"ts_sec": [1000], "open": [10.0], "high": [11.0], "low": [9.0], "close": [10.5], "volume": [100]}
        fb_df = MagicMock()
        curl_mod = types.ModuleType("curl_cffi")
        curl_req = types.ModuleType("curl_cffi.requests")
        curl_req.AsyncSession = MagicMock()
        curl_mod.requests = curl_req

        with (
            patch.dict(sys.modules, {"curl_cffi": curl_mod, "curl_cffi.requests": curl_req}),
            patch.object(yc, "_run_coro_sync", return_value=({"FAST.NS": fb_bundle}, {"SLOW.NS"})),
            patch.object(yc, "_fetch_histories_concurrent", return_value=({"SLOW.NS": fb_df}, set())) as mock_yf,
            patch.object(yc, "_df_to_array_bundle", return_value=fb_bundle),
        ):
            out, failed = yc._fetch_market_data(
                [("FAST.NS", "1d"), ("SLOW.NS", None)],
                interval="1m",
                symbol_ranges={"SLOW.NS": ("2026-11-24", "2026-11-26")},
            )

        self.assertEqual(set(out.keys()), {"FAST.NS", "SLOW.NS"})
        self.assertEqual(failed, set())
        mock_yf.assert_called_once()
        self.assertEqual(mock_yf.call_args.kwargs["symbol_ranges"], {"SLOW.NS": ("2026-11-24", "2026-11-26")})

    def test_run_scan_narrows_cold_range_and_skips_neg_cached_symbols(self):
        prev_day = date(2026, 11, 24)
        now_ist = datetime(2026, 11, 25, 10, 30, tzinfo=h.IST)
        cache_key = (prev_day, "1m")

        with bc._CACHE_LOCK:
            bc._PREV_DAY_MEMORY_CACHE.clear()
            bc._NEG_CACHE.clear()
            bc._PREV_DAY_MEMORY_CACHE[cache_key] = {
                "version": bc.BASELINE_FORMAT_VERSION,
                "prev_day": prev_day,
                "interval": "1m",
                "times_sec": [],
                "symbols": ["CACHED.NS"],
                "symbol_to_idx": {"CACHED.NS": 0},
                "cumvol": [],
                "prev_close": [],
                "prev_full_vol": [],
                "missing_symbols": set(),
            }
            bc._NEG_CACHE[cache_key] = {"NEG.NS": bc.pytime.monotonic() + 300.0}

        captured = {}

        def fake_fetch(symbol_periods, interval, max_workers=8, start=None, end=None, chunk_size=None, symbol_ranges=None):
            captured["tasks"] = list(symbol_periods)
            captured["ranges"] = symbol_ranges
            return {}, set()

        with (
            patch.object(s, "datetime", wraps=datetime) as mock_dt,
            patch.object(s, "get_previous_trading_day", return_value=prev_day),
            patch.object(bc, "_prune_old_cache"),
            patch.object(yc, "_fetch_market_data", side_effect=fake_fetch),
            patch.object(bc, "_commit_baseline_updates", return_value=bc._PREV_DAY_MEMORY_CACHE[cache_key]),
        ):
            mock_dt.now.return_value = now_ist
            s.run_scan(["CACHED.NS", "COLD.NS", "NEG.NS"], interval="1m")

        self.assertEqual(captured["tasks"], [("CACHED.NS", "1d"), ("COLD.NS", None)])
        self.assertEqual(captured["ranges"], {"COLD.NS": ("2026-11-24", "2026-11-26")})

        # Verify warm run_scan uses passed prev_day without calling get_previous_trading_day or _commit_baseline_updates
        with (
            patch.object(s, "datetime", wraps=datetime) as mock_dt2,
            patch.object(s, "get_previous_trading_day") as mock_prev,
            patch.object(yc, "_fetch_market_data", side_effect=fake_fetch),
            patch.object(bc, "_commit_baseline_updates") as mock_commit,
        ):
            mock_dt2.now.return_value = now_ist
            s.run_scan(["CACHED.NS"], interval="1m", prev_day=prev_day)
        mock_prev.assert_not_called()
        mock_commit.assert_not_called()

    def test_pack_baseline_store_numpy_ffill_and_cache_hit_fast_path(self):
        prev_day = date(2026, 11, 24)
        entries = {
            "AAA.NS": {
                "times_sec": bc.np.array([33300, 33360, 33420], dtype=bc.np.int32),
                "cumvol": bc.np.array([100.0, 220.0, 350.0], dtype=bc.np.float64),
                "prev_close": 500.0,
            },
            "BBB.NS": {
                "times_sec": bc.np.array([33360], dtype=bc.np.int32),
                "cumvol": bc.np.array([50.0], dtype=bc.np.float64),
                "prev_close": 250.0,
            },
        }
        packed = bc._pack_baseline_store(entries, prev_day, "1m")
        self.assertEqual(packed["cumvol"].shape, (3, 2))
        self.assertTrue(bc.np.isnan(packed["cumvol"][0, 1]))  # leading NaN preserved
        self.assertEqual(packed["cumvol"][1, 1], 50.0)
        self.assertEqual(packed["cumvol"][2, 1], 50.0)       # trailing gap forward-filled

        cache_key = bc.make_cache_key(prev_day, "1m", "fast_ns")
        with bc._CACHE_LOCK:
            bc._PREV_DAY_MEMORY_CACHE[cache_key] = packed
        try:
            with (
                patch.object(bc, "_get_neg_cached") as mock_neg,
                patch.object(bc, "_commit_baseline_updates") as mock_commit,
            ):
                hit = bc.get_previous_day_data(
                    ["AAA.NS", "BBB.NS"], prev_day, interval="1m", cache_namespace="fast_ns"
                )
            self.assertIs(hit, packed)
            mock_neg.assert_not_called()
            mock_commit.assert_not_called()
        finally:
            with bc._CACHE_LOCK:
                bc._PREV_DAY_MEMORY_CACHE.pop(cache_key, None)

    def test_resolve_scan_session_and_auto_holiday_weekend_preopen_replay(self):
        import glob
        import core.baseline_cache as bc_mod
        import core.holidays as holidays_mod
        import core.scanner as scanner_mod

        ist = holidays_mod.IST
        # Weekend Saturday -> Friday 27th vs Thursday 26th, cutoff 15:30, replay=True
        sat_dt = ist.localize(datetime(2026, 11, 28, 11, 0))
        self.assertEqual(
            scanner_mod.resolve_scan_session(now=sat_dt, holidays_set=set()),
            (date(2026, 11, 27), date(2026, 11, 26), time(15, 30), True),
        )
        # Weekend Sunday -> Friday 27th vs Thursday 26th, cutoff 15:30, replay=True
        sun_dt = ist.localize(datetime(2026, 11, 29, 11, 0))
        self.assertEqual(
            scanner_mod.resolve_scan_session(now=sun_dt, holidays_set=set()),
            (date(2026, 11, 27), date(2026, 11, 26), time(15, 30), True),
        )
        # Weekday trading holiday (Tue 2026-11-24) -> Mon 23rd vs Fri 20th, replay=True
        hol_dt = ist.localize(datetime(2026, 11, 24, 11, 0))
        self.assertEqual(
            scanner_mod.resolve_scan_session(now=hol_dt, holidays_set={date(2026, 11, 24)}),
            (date(2026, 11, 23), date(2026, 11, 20), time(15, 30), True),
        )
        # Trading-day pre-open (09:00 IST) -> Thu 26th vs Wed 25th, replay=True
        pre_dt = ist.localize(datetime(2026, 11, 27, 9, 0))
        self.assertEqual(
            scanner_mod.resolve_scan_session(now=pre_dt, holidays_set=set(), market_open=False),
            (date(2026, 11, 26), date(2026, 11, 25), time(15, 30), True),
        )
        # Trading-day live open (10:30 IST) -> live mode, replay=False
        open_dt = ist.localize(datetime(2026, 11, 27, 10, 30))
        self.assertEqual(
            scanner_mod.resolve_scan_session(now=open_dt, holidays_set=set(), market_open=True),
            (date(2026, 11, 27), date(2026, 11, 26), time(10, 30), False),
        )
        # Trading-day post-close (16:00 IST) -> same day at 15:30, replay=False
        post_dt = ist.localize(datetime(2026, 11, 27, 16, 0))
        self.assertEqual(
            scanner_mod.resolve_scan_session(now=post_dt, holidays_set=set(), market_open=False),
            (date(2026, 11, 27), date(2026, 11, 26), time(15, 30), False),
        )

        # End-to-end run_scan auto-replay verification (skip live summary, pass target_day range, apply 15:30 cutoff)
        ns = f"replay_test_{os.getpid()}"
        fetch_calls = []
        np_mod = scanner_mod.np

        def _ts(d_obj, h_val, m_val):
            return int(ist.localize(datetime.combine(d_obj, time(h_val, m_val))).timestamp())

        def _fake_fetch_bars(symbol_periods, interval="1m", max_workers=8, start=None, end=None, chunk_size=None, symbol_ranges=None):
            pairs = list(symbol_periods)
            fetch_calls.append({"pairs": pairs, "start": start, "end": end, "symbol_ranges": symbol_ranges})
            if start == "2026-11-26" and end == "2026-11-27":
                # Baseline previous day (Thu 2026-11-26)
                ts_arr = np_mod.array([_ts(date(2026, 11, 26), 9, 15), _ts(date(2026, 11, 26), 15, 29)], dtype=np_mod.int64)
                return (
                    {
                        "REPLAY.NS": {
                            "ts_sec": ts_arr,
                            "open": np_mod.array([100.0, 102.0], dtype=np_mod.float64),
                            "high": np_mod.array([101.0, 103.0], dtype=np_mod.float64),
                            "low": np_mod.array([99.0, 101.0], dtype=np_mod.float64),
                            "close": np_mod.array([100.5, 102.5], dtype=np_mod.float64),
                            "volume": np_mod.array([1000.0, 1000.0], dtype=np_mod.float64),
                        }
                    },
                    set(),
                )
            # Replay target day (Fri 2026-11-27), including a 15:58 post-close auction bar that cutoff=15:30 must drop
            ts_arr = np_mod.array(
                [
                    _ts(date(2026, 11, 27), 9, 15),
                    _ts(date(2026, 11, 27), 15, 29),
                    _ts(date(2026, 11, 27), 15, 58),
                ],
                dtype=np_mod.int64,
            )
            return (
                {
                    "REPLAY.NS": {
                        "ts_sec": ts_arr,
                        "open": np_mod.array([103.0, 106.0, 107.0], dtype=np_mod.float64),
                        "high": np_mod.array([105.0, 108.0, 108.0], dtype=np_mod.float64),
                        "low": np_mod.array([102.0, 105.0, 106.0], dtype=np_mod.float64),
                        "close": np_mod.array([104.0, 107.0, 107.5], dtype=np_mod.float64),
                        "volume": np_mod.array([2500.0, 2500.0, 999999.0], dtype=np_mod.float64),
                    }
                },
                set(),
            )

        mock_prov = MagicMock()
        mock_prov.provider_id = "mock_replay"
        mock_prov.display_name = "Mock Replay"
        mock_prov.cache_namespace = ns
        mock_prov.supports_coalesced_range = False
        mock_prov.fetch_today_summaries.side_effect = AssertionError("fetch_today_summaries must be skipped in replay mode")
        mock_prov.fetch_bars.side_effect = _fake_fetch_bars
        mock_prov.get_last_error.return_value = None

        class _FakeSatDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return sat_dt

        cache_key = bc_mod.make_cache_key(date(2026, 11, 26), "1m", ns)
        try:
            with (
                patch.object(scanner_mod, "datetime", _FakeSatDatetime),
                patch.object(scanner_mod, "get_holidays", return_value=set()),
                patch.object(holidays_mod, "get_holidays", return_value=set()),
                patch.object(scanner_mod, "is_market_open", return_value=False),
            ):
                df = scanner_mod.run_scan(
                    ["REPLAY.NS"],
                    min_ratio=1.0,
                    min_volume=0,
                    interval="1m",
                    include_oi=False,
                    provider=mock_prov,
                )

            mock_prov.fetch_today_summaries.assert_not_called()
            self.assertEqual(len(fetch_calls), 2)
            self.assertEqual(fetch_calls[1]["pairs"], [("REPLAY.NS", None)])
            self.assertEqual(
                fetch_calls[1]["symbol_ranges"],
                {"REPLAY.NS": ("2026-11-27", "2026-11-28")},
            )
            self.assertEqual(len(df), 1)
            row = df.iloc[0]
            self.assertEqual(row["Symbol"], "REPLAY")
            self.assertEqual(row["Vol_Today"], 5000)
            self.assertEqual(row["Vol_Prev_Same_Time"], 2000)
            self.assertAlmostEqual(row["Ratio"], 2.5)
            self.assertEqual(row["Last_Bar_Time"], "15:29")
            self.assertAlmostEqual(row["LTP"], 107.0)
            self.assertEqual(
                df.attrs.get("session_meta"),
                {
                    "target_day": "2026-11-27",
                    "prev_day": "2026-11-26",
                    "cutoff": "15:30",
                    "is_replay": True,
                },
            )
            self.assertEqual(df.attrs.get("provider_meta", {}).get("mode"), "replay")
        finally:
            with bc_mod._CACHE_LOCK:
                bc_mod._PREV_DAY_MEMORY_CACHE.pop(cache_key, None)
                bc_mod._NEG_CACHE.pop(cache_key, None)
            for p in glob.glob(str(bc_mod.CACHE_DIR / f"prev_baseline_{ns}_*.pkl")):
                try:
                    os.remove(p)
                except OSError:
                    pass


if __name__ == "__main__":
    unittest.main(verbosity=2)
