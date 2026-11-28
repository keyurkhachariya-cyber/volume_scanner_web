import asyncio
import gzip
import json
import os
import sys
import tempfile
import unittest
from datetime import date, datetime, time
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import core.baseline_cache as bc
import core.holidays as h
import providers.common.http_runtime as hr_mod
import providers.upstox.instruments as ui_mod
import providers.upstox.provider as up_mod
import providers.upstox.stream as us_mod
import core.scanner as scanner


class UpstoxProviderTest(unittest.TestCase):
    def setUp(self):
        with ui_mod._LOCK:
            ui_mod._MEM_CACHE = None
            ui_mod._MEM_CACHE_DATE = None
        with up_mod._SEED_LOCK:
            up_mod._INFLIGHT_SEEDS.clear()
            up_mod._EMPTY_TODAY_UNTIL.clear()
        us_mod.stop_upstox_stream()

    def tearDown(self):
        with up_mod._SEED_LOCK:
            up_mod._INFLIGHT_SEEDS.clear()
            up_mod._EMPTY_TODAY_UNTIL.clear()
        us_mod.stop_upstox_stream()

    def test_instrument_parser_prefers_eq_over_debt_series(self):
        raw_items = [
            {"segment": "NSE_EQ", "trading_symbol": "MOTHERSON", "instrument_type": "D1", "instrument_key": "NSE_EQ|INE775A08105"},
            {"segment": "NSE_EQ", "trading_symbol": "MOTHERSON", "instrument_type": "EQ", "instrument_key": "NSE_EQ|INE775A01035"},
            {"segment": "NSE_EQ", "trading_symbol": "M&M", "instrument_type": "EQ", "instrument_key": "NSE_EQ|INE101A01026"},
            {"segment": "NSE_FO", "trading_symbol": "RELIANCE", "instrument_type": "FUT", "instrument_key": "NSE_FO|99999"},
        ]
        eq_map = ui_mod._parse_instruments_payload(raw_items)
        self.assertEqual(eq_map["MOTHERSON"], "NSE_EQ|INE775A01035")
        self.assertEqual(eq_map["M&M"], "NSE_EQ|INE101A01026")
        self.assertNotIn("RELIANCE", eq_map)

        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "upstox_nse_instruments.json"
            payload_bytes = gzip.compress(json.dumps(raw_items).encode("utf-8"))
            fake_resp = MagicMock(content=payload_bytes)
            fake_resp.raise_for_status = MagicMock()
            with (
                patch.object(ui_mod, "INSTRUMENTS_CACHE_FILE", cache_file),
                patch("requests.get", return_value=fake_resp) as mock_get,
            ):
                m1 = ui_mod.get_upstox_nse_eq_map()
                self.assertEqual(m1["MOTHERSON"], "NSE_EQ|INE775A01035")
                self.assertTrue(cache_file.exists())

            # Clear RAM cache -> next load reads disk without HTTP request
            with ui_mod._LOCK:
                ui_mod._MEM_CACHE = None
                ui_mod._MEM_CACHE_DATE = None
            with (
                patch.object(ui_mod, "INSTRUMENTS_CACHE_FILE", cache_file),
                patch("requests.get", side_effect=AssertionError("Should load from disk")),
            ):
                s2k, k2s = ui_mod.resolve_instrument_keys(["MOTHERSON.NS", "M&M.NS"])
                self.assertEqual(s2k["MOTHERSON.NS"], "NSE_EQ|INE775A01035")
                self.assertEqual(k2s["NSE_EQ|INE101A01026"], "M&M.NS")
                self.assertEqual(mock_get.call_count, 1)

    def test_parse_upstox_candles_sorts_descending_to_ascending(self):
        payload = {
            "status": "success",
            "data": {
                "candles": [
                    ["2026-11-27T09:17:00+05:30", 102.0, 104.0, 101.5, 103.5, 3000, 0],
                    ["2026-11-27T09:16:00+05:30", 101.0, 102.5, 100.5, 102.0, 2000, 0],
                    ["2026-11-27T09:15:00+05:30", 100.0, 101.5, 99.5, 101.0, 1000, 0],
                ]
            },
        }
        bundle, ok = up_mod._parse_upstox_candles(payload)
        self.assertTrue(ok)
        self.assertIsNotNone(bundle)
        self.assertTrue((np.diff(bundle["ts_sec"]) > 0).all())
        self.assertEqual(list(bundle["close"]), [101.0, 102.0, 103.5])
        self.assertEqual(list(bundle["volume"]), [1000.0, 2000.0, 3000.0])

        err_bundle, err_ok = up_mod._parse_upstox_candles({"status": "error"})
        self.assertIsNone(err_bundle)
        self.assertFalse(err_ok)

    def test_oauth_url_and_token_exchange(self):
        url = up_mod.build_upstox_login_url("KEY123", "http://localhost:8501")
        self.assertIn("client_id=KEY123", url)
        self.assertIn("response_type=code", url)

        ok_resp = MagicMock(status_code=200)
        ok_resp.json.return_value = {"access_token": "tok_abc_123"}
        with patch("requests.post", return_value=ok_resp):
            tok, err = up_mod.exchange_upstox_auth_code("CODE1", "CID", "CSEC", "http://localhost:8501")
        self.assertEqual(tok, "tok_abc_123")
        self.assertIsNone(err)

        bad_resp = MagicMock(status_code=401)
        bad_resp.json.return_value = {"errors": [{"message": "Invalid auth code"}]}
        with patch("requests.post", return_value=bad_resp):
            tok2, err2 = up_mod.exchange_upstox_auth_code("BAD", "CID", "CSEC", "http://localhost:8501")
        self.assertIsNone(tok2)
        self.assertIn("Invalid auth code", err2)

    def test_upstox_stream_ingest_seed_and_stale_date_guard(self):
        today = date(2026, 11, 27)
        prev_day = date(2026, 11, 26)
        open_dt = h.IST.localize(datetime(2026, 11, 27, 9, 15))
        open_sec = int(open_dt.timestamp())

        stream = us_mod.UpstoxMarketStream("dummy_tok", {"NSE_EQ|1": "RELIANCE.NS"})
        # Seed 30 bars (first 29 bars = 1000 vol/min, 30th at 09:44 = 5000)
        ts_seed = np.array([open_sec + i * 60 for i in range(30)], dtype=np.int64)
        vol_seed = np.array([1000.0] * 15 + [5000.0] * 15, dtype=np.float64)
        stream.seed_from_intraday_bundles(
            today,
            {
                "RELIANCE.NS": {
                    "ts_sec": ts_seed,
                    "open": np.full(30, 1500.0),
                    "high": np.full(30, 1510.0),
                    "low": np.full(30, 1495.0),
                    "close": np.full(30, 1505.0),
                    "volume": vol_seed,
                }
            },
        )

        # Ingest live Protobuf-style MarketFullFeed message (note int64 fields serialized as strings)
        tick_sec = open_sec + 30 * 60  # 09:45
        live_msg = {
            "type": "live_feed",
            "currentTs": str(tick_sec * 1000),
            "feeds": {
                "NSE_EQ|1": {
                    "fullFeed": {
                        "marketFF": {
                            "ltpc": {"ltp": 1525.0, "ltt": str(tick_sec * 1000), "cp": 1480.0},
                            "marketOHLC": {
                                "ohlc": [
                                    {"interval": "1d", "open": 1500.0, "high": 1528.0, "low": 1492.0, "close": 1525.0, "vol": "96000", "ts": str(open_sec * 1000)},
                                    {"interval": "I1", "open": 1520.0, "high": 1525.0, "low": 1519.0, "close": 1525.0, "vol": "6000", "ts": str(tick_sec * 1000)},
                                ]
                            },
                            "atp": 1512.4,
                            "vtt": "96000",
                        }
                    }
                }
            },
        }
        stream._on_message(live_msg)
        summaries = stream.get_today_summaries(["RELIANCE.NS"], today, time(9, 46), bar_interval_sec=60)
        self.assertIn("RELIANCE.NS", summaries)
        vol_t, ltp, d_op, d_hi, d_lo, lbt, vwap, burst = summaries["RELIANCE.NS"]
        self.assertEqual(vol_t, 96000.0)
        self.assertEqual(ltp, 1525.0)
        self.assertEqual(d_op, 1500.0)
        self.assertEqual(d_hi, 1528.0)
        self.assertEqual(d_lo, 1492.0)
        self.assertEqual(lbt, time(9, 45))
        self.assertAlmostEqual(vwap, 1512.4)
        self.assertGreater(burst, 1.5)

        # Previous trading day's stale feed timestamp must NOT leak into today's live summaries
        stale_sec = int(h.IST.localize(datetime(2026, 11, 26, 15, 29)).timestamp())
        stream_stale = us_mod.UpstoxMarketStream("dummy_tok2", {"NSE_EQ|2": "TCS.NS"})
        stream_stale._on_message(
            {
                "type": "initial_feed",
                "currentTs": str(stale_sec * 1000),
                "feeds": {
                    "NSE_EQ|2": {
                        "fullFeed": {
                            "marketFF": {
                                "ltpc": {"ltp": 4000.0, "ltt": str(stale_sec * 1000)},
                                "marketOHLC": {"ohlc": [{"interval": "1d", "open": 3990.0, "high": 4010.0, "low": 3980.0, "vol": "50000", "ts": str(stale_sec * 1000)}]},
                                "atp": 3995.0,
                                "vtt": "50000",
                            }
                        }
                    }
                },
            }
        )
        self.assertEqual(stream_stale.get_today_summaries(["TCS.NS"], today, time(9, 20), 60), {})
        self.assertNotIn("TCS.NS", stream_stale.get_today_summaries(["TCS.NS"], prev_day + (today - prev_day), time(9, 20), 60))

    def test_run_scan_with_upstox_provider_uses_isolated_cache_namespace(self):
        today = date(2026, 11, 27)
        prev_day = date(2026, 11, 26)
        now_ist = h.IST.localize(datetime(2026, 11, 27, 10, 0))
        cache_key = bc.make_cache_key(prev_day, "1m", "upstox")
        self.assertEqual(cache_key, ("upstox", prev_day, "1m"))

        fake_store = {
            "version": bc.BASELINE_FORMAT_VERSION,
            "prev_day": prev_day,
            "interval": "1m",
            "times_sec": np.array([9 * 3600 + 59 * 60], dtype=np.int64),
            "symbols": ["RELIANCE.NS"],
            "symbol_to_idx": {"RELIANCE.NS": 0},
            "cumvol": np.array([[20000.0]], dtype=float),
            "prev_close": np.array([1500.0], dtype=float),
            "prev_full_vol": np.array([80000.0], dtype=float),
            "missing_symbols": set(),
        }
        fake_prov = up_mod.UpstoxProvider("tok_x", use_live_ws=True, eq_map={"RELIANCE": "NSE_EQ|1"})
        fake_summary = (50000.0, 1545.0, 1510.0, 1550.0, 1508.0, time(9, 59), 1532.0, 1.9)

        with bc._CACHE_LOCK:
            orig_mem = dict(bc._PREV_DAY_MEMORY_CACHE)
            bc._PREV_DAY_MEMORY_CACHE.clear()
            bc._PREV_DAY_MEMORY_CACHE[cache_key] = fake_store
        try:
            with (
                patch.object(scanner, "datetime", wraps=datetime) as mock_dt,
                patch.object(fake_prov, "fetch_today_summaries", return_value=({"RELIANCE.NS": fake_summary}, {"provider": "upstox", "mode": "wss", "ws_connected": True, "ticks": 42})),
            ):
                mock_dt.now.return_value = now_ist
                df = scanner.run_scan(["RELIANCE.NS"], min_ratio=0.0, min_volume=0, interval="1m", prev_day=prev_day, include_oi=False, provider=fake_prov)
        finally:
            with bc._CACHE_LOCK:
                bc._PREV_DAY_MEMORY_CACHE.clear()
                bc._PREV_DAY_MEMORY_CACHE.update(orig_mem)

        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["Symbol"], "RELIANCE")
        self.assertEqual(df.iloc[0]["Ratio"], 2.5)
        self.assertEqual(df.attrs["provider_meta"]["mode"], "wss")
        self.assertTrue(df.attrs["provider_meta"]["ws_connected"])

    def test_batch_quotes_parser_and_rest_mode_plus_closed_cooldown(self):
        today = date(2026, 11, 27)
        live_ms = int(h.IST.localize(datetime(2026, 11, 27, 10, 15, 0)).timestamp() * 1000)
        stale_ms = int(h.IST.localize(datetime(2026, 11, 26, 15, 29, 0)).timestamp() * 1000)
        key_to_sym = {"NSE_EQ|1": "RELIANCE.NS", "NSE_EQ|2": "TCS.NS"}

        payload = {
            "status": "success",
            "data": {
                "NSE_EQ:RELIANCE": {
                    "instrument_token": "NSE_EQ|1",
                    "symbol": "RELIANCE",
                    "last_price": 1525.5,
                    "volume": 90000,
                    "average_price": 1518.2,
                    "ohlc": {"open": 1500.0, "high": 1530.0, "low": 1495.0, "close": 1525.5},
                    "timestamp": "2026-11-27T10:15:02+05:30",
                    "last_trade_time": str(live_ms),
                },
                # Even though server timestamp is today morning, last_trade_time is yesterday -> reject
                "NSE_EQ:TCS": {
                    "instrument_token": "NSE_EQ|2",
                    "symbol": "TCS",
                    "last_price": 4000.0,
                    "volume": 50000,
                    "average_price": 3995.0,
                    "ohlc": {"open": 3990.0, "high": 4010.0, "low": 3980.0, "close": 4000.0},
                    "timestamp": "2026-11-27T09:05:00+05:30",
                    "last_trade_time": str(stale_ms),
                },
            },
        }
        snaps = up_mod._parse_upstox_batch_quotes(payload, key_to_sym, today)
        self.assertEqual(len(snaps), 1)
        self.assertEqual(snaps[0]["sym"], "RELIANCE.NS")
        self.assertAlmostEqual(snaps[0]["atp"], 1518.2)

        prov = up_mod.UpstoxProvider("tok_fast", use_live_ws=False, eq_map={"RELIANCE": "NSE_EQ|1", "TCS": "NSE_EQ|2"})
        with (
            patch.object(up_mod, "is_market_open", return_value=True),
            patch.object(prov, "_fetch_batch_quotes_sync", return_value=(snaps, True)) as mock_bq,
        ):
            summaries, meta = prov.fetch_today_summaries(["RELIANCE.NS", "TCS.NS"], today, time(10, 20), "1m")
        self.assertEqual(mock_bq.call_count, 1)
        self.assertEqual(meta["mode"], "batch_quote")
        self.assertIn("RELIANCE.NS", summaries)
        self.assertEqual(summaries["RELIANCE.NS"][0], 90000.0)
        self.assertAlmostEqual(summaries["RELIANCE.NS"][6], 1518.2)

        # Closed market + empty batch quotes -> enters 30s cooldown so second click makes 0 network calls
        prov_closed = up_mod.UpstoxProvider("tok_closed", use_live_ws=False, eq_map={"RELIANCE": "NSE_EQ|1"})
        with (
            patch.object(up_mod, "is_market_open", return_value=False),
            patch.object(prov_closed, "_fetch_batch_quotes_sync", return_value=([], True)) as mock_closed_bq,
        ):
            s1, m1 = prov_closed.fetch_today_summaries(["RELIANCE.NS"], today, time(10, 20), "1m")
            s2, m2 = prov_closed.fetch_today_summaries(["RELIANCE.NS"], today, time(10, 21), "1m")
        self.assertEqual(mock_closed_bq.call_count, 1)
        self.assertEqual(s1, {})
        self.assertEqual(s2, {})
        self.assertEqual(m1["mode"], "closed_cached")
        self.assertEqual(m2["mode"], "closed_cached")

    def test_sliding_rate_limiter_and_shared_fetch_urls_retry(self):
        limiter = hr_mod.AsyncSlidingRateLimiter(max_per_sec=10, max_per_min=100)
        asyncio.run(limiter.acquire())

        calls = {"n": 0}

        class _FakeResp:
            def __init__(self, code, payload):
                self.status_code = code
                self._payload = payload
                self.content = json.dumps(payload).encode("utf-8")

            def json(self):
                return self._payload

        class _FakeSess:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

            async def get(self, url, headers=None, timeout=10):
                calls["n"] += 1
                if calls["n"] == 1:
                    return _FakeResp(429, {})
                return _FakeResp(
                    200,
                    {"status": "success", "data": {"candles": [["2026-11-27T09:15:00+05:30", 10, 11, 9, 10.5, 500, 0]]}},
                )

        rl_hits = []
        with patch("curl_cffi.requests.AsyncSession", return_value=_FakeSess()):
            out, failed = asyncio.run(
                hr_mod.fetch_json_urls_async(
                    sym_urls=[("RELIANCE.NS", "https://api.upstox.com/test")],
                    headers={"Authorization": "Bearer x"},
                    parser_fn=up_mod._parse_upstox_candles,
                    max_concurrency=4,
                    max_per_sec=20,
                    max_per_min=100,
                    limiter_key="test_k",
                    on_auth_error=lambda: None,
                    on_rate_limit=lambda: rl_hits.append(1),
                    should_abort=lambda: False,
                )
            )
        self.assertEqual(len(rl_hits), 1)
        self.assertEqual(failed, set())
        self.assertIn("RELIANCE.NS", out)

    def test_after_market_auto_stops_ws_and_market_guard_shutdown(self):
        import providers.common.stream_base as sb_mod

        today = date(2026, 11, 27)
        close_sec = int(h.IST.localize(datetime(2026, 11, 27, 15, 29)).timestamp())
        stream = us_mod.UpstoxMarketStream("tok_evening", {"NSE_EQ|1": "RELIANCE.NS"})
        stream._started = True
        stream._connected = True
        stream._quotes_by_sym["RELIANCE.NS"] = {
            "feed_date": today,
            "ltp": 1525.0,
            "day_open": 1500.0,
            "day_high": 1530.0,
            "day_low": 1495.0,
            "atp": 1515.0,
            "vtt": 95000.0,
            "last_vtt": 95000.0,
            "last_ts_sec": close_sec,
            "minute_vols": {},
            "pv_sum": 0.0,
            "pv_vol": 0.0,
        }
        with us_mod._STREAM_LOCK:
            us_mod._ACTIVE_STREAMS[us_mod._token_fingerprint("tok_evening")] = stream

        prov = up_mod.UpstoxProvider("tok_evening", use_live_ws=True, eq_map={"RELIANCE": "NSE_EQ|1"})
        with (
            patch.object(up_mod, "is_market_open", return_value=False),
            patch.object(up_mod, "is_wss_active_window", return_value=False),
            patch.object(stream, "stop", wraps=stream.stop) as mock_stop,
        ):
            summaries, meta = prov.fetch_today_summaries(["RELIANCE.NS"], today, time(16, 10), "1m")

        self.assertEqual(mock_stop.call_count, 1)
        self.assertFalse(stream._started)
        self.assertEqual(meta["mode"], "post_close_cached")
        self.assertIn("RELIANCE.NS", summaries)
        self.assertEqual(summaries["RELIANCE.NS"][0], 95000.0)

        # Guard helper stops all registered streams once outside NSE WSS window
        stopped = []
        with sb_mod._GUARD_LOCK:
            sb_mod._GUARD_STOPPERS["test_after_close"] = lambda: stopped.append("yes")
        with patch.object(sb_mod, "is_wss_active_window", return_value=False):
            self.assertTrue(sb_mod.enforce_market_hours_shutdown())
        self.assertEqual(stopped, ["yes"])

    def test_replay_coalesced_range_for_upstox_and_fivepaisa_plus_429_recovery_clears_error(self):
        import glob
        import providers.fivepaisa.provider as fp_mod

        self.assertTrue(up_mod.UpstoxProvider.supports_replay_coalesced_range)
        self.assertTrue(fp_mod.FivePaisaProvider.supports_replay_coalesced_range)
        self.assertFalse(up_mod.UpstoxProvider.supports_coalesced_range)
        self.assertFalse(fp_mod.FivePaisaProvider.supports_coalesced_range)

        # 1. Shared rate limiter tracks 30m cap & shared penalize cooldown
        limiter = hr_mod.AsyncSlidingRateLimiter(max_per_sec=10, max_per_min=100, max_per_30min=500)
        self.assertGreater(asyncio.run(limiter.penalize(0.6)), 0.0)
        self.assertGreater(limiter.remaining_cooldown(), 0.0)

        # 2. HTTP 429 with Retry-After recovers in Wave 2/3 and clears provider error; persistent 429 retains it
        class _FakeResp:
            def __init__(self, code, payload, headers=None):
                self.status_code = code
                self._payload = payload
                self.headers = headers or {}
                self.content = json.dumps(payload).encode("utf-8")

            def json(self):
                return self._payload

        def _run_fetch_with_codes(codes):
            idx = {"i": 0}

            class _FakeSess:
                async def __aenter__(self):
                    return self

                async def __aexit__(self, *args):
                    return False

                async def get(self, url, headers=None, timeout=10):
                    pos = min(idx["i"], len(codes) - 1)
                    idx["i"] += 1
                    c = codes[pos]
                    if c == 429:
                        return _FakeResp(429, {}, headers={"Retry-After": "0.5"})
                    return _FakeResp(
                        200,
                        {"status": "success", "data": {"candles": [["2026-11-27T09:15:00+05:30", 10, 11, 9, 10.5, 500, 0]]}},
                    )

            prov_rl = up_mod.UpstoxProvider("tok_rl", use_live_ws=False, eq_map={"RELIANCE": "NSE_EQ|1"})

            async def _noop_sleep(_s):
                return None

            def _direct_coro(coro_fn):
                return asyncio.run(coro_fn())

            with (
                patch("curl_cffi.requests.AsyncSession", return_value=_FakeSess()),
                patch("providers.common.http_runtime.yahoo_client._ASYNC_LOOP", new=None),
                patch.object(up_mod.yahoo_client, "_run_coro_sync", side_effect=_direct_coro),
                patch("asyncio.sleep", side_effect=_noop_sleep),
            ):
                return prov_rl, prov_rl.fetch_bars([("RELIANCE.NS", "1d")], interval="1m")

        prov_ok, (res_ok, fail_ok) = _run_fetch_with_codes([429, 429, 200])  # recovers on Wave 3!
        self.assertIn("RELIANCE.NS", res_ok)
        self.assertEqual(fail_ok, set())
        self.assertIsNone(prov_ok.get_last_error())

        prov_bad, (res_bad, fail_bad) = _run_fetch_with_codes([429, 429, 429])  # fails all waves -> retains error
        self.assertEqual(res_bad, {})
        self.assertEqual(fail_bad, {"RELIANCE.NS"})
        self.assertIn("429", prov_bad.get_last_error() or "")

        # 3. Cold replay scan coalesces (prev_day -> tomorrow) into ONE fetch_bars call on Upstox & 5paisa
        sat_dt = h.IST.localize(datetime(2026, 11, 28, 11, 0))
        prev_day = date(2026, 11, 26)
        ts_26_1 = int(h.IST.localize(datetime(2026, 11, 26, 9, 15)).timestamp())
        ts_26_2 = int(h.IST.localize(datetime(2026, 11, 26, 15, 29)).timestamp())
        ts_27_1 = int(h.IST.localize(datetime(2026, 11, 27, 9, 15)).timestamp())
        ts_27_2 = int(h.IST.localize(datetime(2026, 11, 27, 15, 29)).timestamp())
        combined_bundle = {
            "ts_sec": np.array([ts_26_1, ts_26_2, ts_27_1, ts_27_2], dtype=np.int64),
            "open": np.array([100.0, 101.0, 102.0, 105.0], dtype=np.float64),
            "high": np.array([101.0, 102.0, 104.0, 106.0], dtype=np.float64),
            "low": np.array([99.0, 100.0, 101.0, 104.0], dtype=np.float64),
            "close": np.array([100.5, 101.5, 103.0, 105.5], dtype=np.float64),
            "volume": np.array([1000.0, 1000.0, 2000.0, 3000.0], dtype=np.float64),
        }

        class _FakeSatDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return sat_dt

        for idx, prov_cls in enumerate((up_mod.UpstoxProvider, fp_mod.FivePaisaProvider)):
            ns = f"coalesce_replay_{idx}_{os.getpid()}"
            prov_obj = (
                prov_cls("tok_c", use_live_ws=False, eq_map={"RELIANCE": "NSE_EQ|1"})
                if prov_cls is up_mod.UpstoxProvider
                else prov_cls("tok_c", use_live_ws=False, eq_map={"RELIANCE": "2885"}, user_key="UKEY")
            )
            prov_obj.cache_namespace = ns
            fetch_calls = []

            def _fake_bars(symbol_periods, interval="1m", max_workers=8, start=None, end=None, chunk_size=None, symbol_ranges=None):
                fetch_calls.append((list(symbol_periods), start, end, symbol_ranges))
                return {"RELIANCE.NS": combined_bundle}, set()

            cache_key = bc.make_cache_key(prev_day, "1m", ns)
            try:
                with (
                    patch.object(scanner, "datetime", _FakeSatDatetime),
                    patch.object(scanner, "get_holidays", return_value=set()),
                    patch.object(h, "get_holidays", return_value=set()),
                    patch.object(scanner, "is_market_open", return_value=False),
                    patch.object(prov_obj, "fetch_bars", side_effect=_fake_bars),
                ):
                    df = scanner.run_scan(["RELIANCE.NS"], min_ratio=0.0, min_volume=0, interval="1m", provider=prov_obj)

                self.assertEqual(len(fetch_calls), 1, f"{prov_cls.__name__} should coalesce cold replay into 1 fetch_bars call")
                self.assertEqual(fetch_calls[0][3], {"RELIANCE.NS": ("2026-11-26", "2026-11-28")})
                self.assertEqual(len(df), 1)
                self.assertEqual(df.iloc[0]["Vol_Today"], 5000)
                self.assertEqual(df.iloc[0]["Vol_Prev_Same_Time"], 2000)
                self.assertAlmostEqual(df.iloc[0]["Ratio"], 2.5)
            finally:
                with bc._CACHE_LOCK:
                    bc._PREV_DAY_MEMORY_CACHE.pop(cache_key, None)
                    bc._NEG_CACHE.pop(cache_key, None)
                for p in glob.glob(str(bc.CACHE_DIR / f"prev_baseline_{ns}_*.pkl")):
                    try:
                        os.remove(p)
                    except OSError:
                        pass


if __name__ == "__main__":
    unittest.main(verbosity=2)
