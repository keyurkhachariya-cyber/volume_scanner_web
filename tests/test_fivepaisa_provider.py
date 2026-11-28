import base64
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
import providers.fivepaisa.instruments as fi_mod
import providers.fivepaisa.provider as fp_mod
import providers.fivepaisa.stream as fs_mod
import core.scanner as scanner


def _make_fake_jwt(claims: dict) -> str:
    hdr = base64.urlsafe_b64encode(b'{"alg":"HS256","typ":"JWT"}').decode("ascii").rstrip("=")
    body = base64.urlsafe_b64encode(json.dumps(claims).encode("utf-8")).decode("ascii").rstrip("=")
    return f"{hdr}.{body}.sig"


class FivePaisaProviderTest(unittest.TestCase):
    def setUp(self):
        with fi_mod._LOCK:
            fi_mod._MEM_CACHE = None
            fi_mod._MEM_CACHE_DATE = None
            fi_mod._FALLBACK_RETRY_AT = 0.0
        with fp_mod._SEED_LOCK:
            fp_mod._INFLIGHT_SEEDS.clear()
            fp_mod._EMPTY_TODAY_UNTIL.clear()
        fs_mod.stop_fivepaisa_stream()

    def tearDown(self):
        with fi_mod._LOCK:
            fi_mod._MEM_CACHE = None
            fi_mod._MEM_CACHE_DATE = None
            fi_mod._FALLBACK_RETRY_AT = 0.0
        with fp_mod._SEED_LOCK:
            fp_mod._INFLIGHT_SEEDS.clear()
            fp_mod._EMPTY_TODAY_UNTIL.clear()
        fs_mod.stop_fivepaisa_stream()

    def test_scripmaster_csv_parser_prefers_eq_and_legacy_aliases(self):
        csv_text = "\n".join(
            [
                "Exch,ExchType,Scripcode,Name,Series,ISIN,FullName",
                "N,C,99999,MOTHERSON,BE,INE775A01035,MOTHERSON BE",
                "N,C,4204,MOTHERSON,EQ,INE775A01035,SAMVRDHNA MTHRSN INTL LTD",
                "N,C,2031,M&M,EQ,INE101A01026,MAHINDRA & MAHINDRA LTD",
                "N,C,5097,ZOMATO,EQ,INE758T01015,ZOMATO LIMITED",
                "N,D,88888,RELIANCE,XX,,RELIANCE FUT",
                "B,C,500325,RELIANCE,EQ,INE002A01018,RELIANCE BSE",
            ]
        )
        eq_map = fi_mod._parse_scripmaster_csv(csv_text)
        self.assertEqual(eq_map["MOTHERSON"], "4204")
        self.assertEqual(eq_map["M&M"], "2031")
        self.assertNotIn("RELIANCE", eq_map)

        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "fivepaisa_nse_instruments.json"
            fail_resp = MagicMock(status_code=404, content=b"")
            fail_resp.raise_for_status.side_effect = RuntimeError("404 Cache not available")
            ok_resp = MagicMock(status_code=200, content=csv_text.encode("utf-8"))
            ok_resp.raise_for_status = MagicMock()
            with (
                patch.object(fi_mod, "INSTRUMENTS_CACHE_FILE", cache_file),
                patch("requests.get", side_effect=[fail_resp, fail_resp, ok_resp]) as mock_get,
            ):
                loaded = fi_mod.get_fivepaisa_nse_eq_map()
                self.assertEqual(loaded["MOTHERSON"], "4204")
                self.assertTrue(cache_file.exists())
                self.assertEqual(mock_get.call_count, 3)

            with fi_mod._LOCK:
                fi_mod._MEM_CACHE = None
                fi_mod._MEM_CACHE_DATE = None
            with (
                patch.object(fi_mod, "INSTRUMENTS_CACHE_FILE", cache_file),
                patch("requests.get", side_effect=AssertionError("Should load from disk")),
            ):
                s2c, c2s = fi_mod.resolve_scrip_codes(["MOTHERSON.NS", "M&M.NS", "ETERNAL.NS"])
                self.assertEqual(s2c["MOTHERSON.NS"], "4204")
                self.assertEqual(s2c["ETERNAL.NS"], "5097")
                self.assertEqual(c2s["2031"], "M&M.NS")

            # After fallback cooldown expires, live segment/nse_eq upgrades the cache to source='live'
            live_csv = csv_text + "\nN,C,757645,ATHERENERG,EQ,INE0LEZ01016,ATHER ENERGY"
            live_resp = MagicMock(status_code=200, content=live_csv.encode("utf-8"))
            live_resp.raise_for_status = MagicMock()
            with fi_mod._LOCK:
                fi_mod._MEM_CACHE = None
                fi_mod._MEM_CACHE_DATE = None
                fi_mod._FALLBACK_RETRY_AT = 0.0
            with (
                patch.object(fi_mod, "INSTRUMENTS_CACHE_FILE", cache_file),
                patch("requests.get", side_effect=[live_resp]) as mock_upgrade,
            ):
                upgraded = fi_mod.get_fivepaisa_nse_eq_map()
            self.assertEqual(mock_upgrade.call_count, 1)
            self.assertEqual(upgraded["ATHERENERG"], "757645")
            saved_payload = json.loads(cache_file.read_text(encoding="utf-8"))
            self.assertEqual(saved_payload["source"], "live")

    def test_totp_rfc6238_login_and_oauth_helpers(self):
        # Standard RFC 6238 SHA-1 Base32 test key ("12345678901234567890") at t=59 -> 94287082 -> 6-digit "287082"
        self.assertEqual(fp_mod.generate_totp_code("GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ", for_time=59), "287082")
        self.assertEqual(fp_mod.generate_totp_code("654321"), "654321")

        login_url = fp_mod.build_fivepaisa_login_url("VKEY123", "http://localhost:8501", state="ok")
        self.assertIn("VendorKey=VKEY123", login_url)
        self.assertIn("ResponseURL=", login_url)

        jwt_tok = _make_fake_jwt({"ClientCode": "50011110", "RedirectServer": "B"})
        totp_resp = MagicMock(status_code=200)
        totp_resp.json.return_value = {"body": {"Status": 0, "RequestToken": "REQ_999"}}
        access_resp = MagicMock(status_code=200)
        access_resp.json.return_value = {
            "body": {"Message": "Success", "AccessToken": jwt_tok, "ClientCode": "50011110"}
        }
        with patch("requests.post", side_effect=[totp_resp, access_resp]) as mock_post:
            tok, ccode, err = fp_mod.login_fivepaisa_totp(
                "50011110", "654321", "1111", "UKEY", "EKEY", "UID1"
            )
        self.assertEqual(tok, jwt_tok)
        self.assertEqual(ccode, "50011110")
        self.assertIsNone(err)
        self.assertEqual(mock_post.call_count, 2)

        bad_resp = MagicMock(status_code=200)
        bad_resp.json.return_value = {"body": {"Status": 1, "Message": "Invalid TOTP"}}
        with patch("requests.post", return_value=bad_resp):
            tok2, _, err2 = fp_mod.login_fivepaisa_totp("50011110", "654321", "1111", "UKEY", "EKEY", "UID1")
        self.assertIsNone(tok2)
        self.assertIn("Invalid TOTP", err2)

    def test_parse_fivepaisa_candles_naive_ist_and_sorting(self):
        payload = {
            "status": "success",
            "data": {
                "candles": [
                    ["2026-11-27T09:17:00", 102.0, 104.0, 101.5, 103.5, 3000],
                    ["2026-11-27T09:15:00", 100.0, 101.5, 99.5, 101.0, 1000],
                    ["2026-11-27T09:16:00", 101.0, 102.5, 100.5, 102.0, 2000],
                ]
            },
        }
        bundle, ok = fp_mod._parse_fivepaisa_candles(payload)
        self.assertTrue(ok)
        self.assertIsNotNone(bundle)
        expected_0915 = int(h.IST.localize(datetime(2026, 11, 27, 9, 15)).timestamp())
        self.assertEqual(int(bundle["ts_sec"][0]), expected_0915)
        self.assertTrue((np.diff(bundle["ts_sec"]) > 0).all())
        self.assertEqual(list(bundle["volume"]), [1000.0, 2000.0, 3000.0])

        err_bundle, err_ok = fp_mod._parse_fivepaisa_candles({"status": "error"})
        self.assertIsNone(err_bundle)
        self.assertFalse(err_ok)

    def test_fivepaisa_stream_ws_routing_batches_running_vwap_and_stale_guard(self):
        tok_a = _make_fake_jwt({"RedirectServer": "A", "unique_name": "50011110"})
        self.assertTrue(
            fs_mod.resolve_fivepaisa_ws_url(tok_a, "50011110").startswith("wss://aopenfeed.5paisa.com/feeds/api/chat?")
        )
        tok_b = _make_fake_jwt({"RedirectServer": "B"})
        self.assertTrue(
            fs_mod.resolve_fivepaisa_ws_url(tok_b, "50011110").startswith("wss://bopenfeed.5paisa.com/feeds/api/chat?")
        )
        self.assertEqual(fs_mod.extract_client_code_from_jwt(tok_a), "50011110")

        batches = fs_mod.build_market_feed_subscribe_payloads("50011110", [str(i) for i in range(1, 121)], batch_size=50)
        self.assertEqual([len(b["MarketFeedData"]) for b in batches], [50, 50, 20])
        self.assertEqual(batches[0]["Method"], "MarketFeedV3")
        self.assertEqual(batches[0]["Operation"], "Subscribe")
        self.assertEqual(batches[0]["MarketFeedData"][0], {"Exch": "N", "ExchType": "C", "ScripCode": 1})

        today = date(2026, 11, 27)
        open_dt = h.IST.localize(datetime(2026, 11, 27, 9, 15))
        open_sec = int(open_dt.timestamp())

        stream = fs_mod.FivePaisaMarketStream(tok_a, "50011110", {"2885": "RELIANCE.NS"})
        ts_seed = np.array([open_sec + i * 60 for i in range(30)], dtype=np.int64)
        vol_seed = np.array([1000.0] * 15 + [3000.0] * 15, dtype=np.float64)
        stream.seed_from_intraday_bundles(
            today,
            {
                "RELIANCE.NS": {
                    "ts_sec": ts_seed,
                    "open": np.full(30, 1500.0),
                    "high": np.full(30, 1510.0),
                    "low": np.full(30, 1490.0),
                    "close": np.full(30, 1500.0),  # tp = (1510+1490+1500)/3 = 1500.0; sum = 60,000 shares
                    "volume": vol_seed,
                }
            },
        )

        # Feed list-style live tick without AverageRate -> updates TotalQty, rolling burst, & running VWAP
        tick_sec = open_sec + 30 * 60  # 09:45
        tick_list_json = json.dumps(
            [
                {
                    "Exch": "N",
                    "ExchType": "C",
                    "Token": 2885,
                    "LastRate": 1560.0,
                    "TotalQty": 70000,  # +10,000 delta vol in 09:45 bucket at LTP 1560 -> VWAP = (60k*1500 + 10k*1560)/70k
                    "High": 1565.0,
                    "Low": 1490.0,
                    "PClose": 1480.0,
                    "TickDt": f"/Date({tick_sec * 1000}+0530)/",
                }
            ]
        )
        stream._on_message(tick_list_json)
        summaries = stream.get_today_summaries(["RELIANCE.NS"], today, time(9, 46), bar_interval_sec=60)
        self.assertIn("RELIANCE.NS", summaries)
        vol_t, ltp, d_op, d_hi, d_lo, lbt, vwap, burst = summaries["RELIANCE.NS"]
        self.assertEqual(vol_t, 70000.0)
        self.assertEqual(ltp, 1560.0)
        self.assertEqual(d_op, 1500.0)
        self.assertEqual(d_hi, 1565.0)
        self.assertEqual(d_lo, 1490.0)
        self.assertEqual(lbt, time(9, 45))
        expected_vwap = (60000.0 * 1500.0 + 10000.0 * 1560.0) / 70000.0
        self.assertAlmostEqual(vwap, expected_vwap, places=4)
        self.assertGreater(burst, 1.5)

        # Stale previous trading day tick must not leak into today's live summaries
        stale_sec = int(h.IST.localize(datetime(2026, 11, 26, 15, 29)).timestamp())
        stream_stale = fs_mod.FivePaisaMarketStream(tok_b, "50011110", {"11536": "TCS.NS"})
        stream_stale._on_message(
            {
                "Data": [
                    {
                        "Token": 11536,
                        "LastRate": 4000.0,
                        "TotalQty": 50000,
                        "TickDt": f"/Date({stale_sec * 1000}+0530)/",
                    }
                ]
            }
        )
        self.assertEqual(stream_stale.get_today_summaries(["TCS.NS"], today, time(9, 20), 60), {})

    def test_run_scan_with_fivepaisa_provider_uses_isolated_cache_namespace(self):
        today = date(2026, 11, 27)
        prev_day = date(2026, 11, 26)
        now_ist = h.IST.localize(datetime(2026, 11, 27, 10, 0))
        cache_key = bc.make_cache_key(prev_day, "1m", "fivepaisa")
        self.assertEqual(cache_key, ("fivepaisa", prev_day, "1m"))

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
        tok = _make_fake_jwt({"ClientCode": "50011110", "RedirectServer": "C"})
        fake_prov = fp_mod.FivePaisaProvider(tok, use_live_ws=True, eq_map={"RELIANCE": "2885"})
        fake_summary = (50000.0, 1545.0, 1510.0, 1550.0, 1508.0, time(9, 59), 1532.0, 1.9)

        with bc._CACHE_LOCK:
            orig_mem = dict(bc._PREV_DAY_MEMORY_CACHE)
            bc._PREV_DAY_MEMORY_CACHE.clear()
            bc._PREV_DAY_MEMORY_CACHE[cache_key] = fake_store
        try:
            with (
                patch.object(scanner, "datetime", wraps=datetime) as mock_dt,
                patch.object(
                    fake_prov,
                    "fetch_today_summaries",
                    return_value=(
                        {"RELIANCE.NS": fake_summary},
                        {"provider": "fivepaisa", "mode": "wss", "ws_connected": True, "ticks": 25},
                    ),
                ),
            ):
                mock_dt.now.return_value = now_ist
                df = scanner.run_scan(
                    ["RELIANCE.NS"],
                    min_ratio=0.0,
                    min_volume=0,
                    interval="1m",
                    prev_day=prev_day,
                    include_oi=False,
                    provider=fake_prov,
                )
        finally:
            with bc._CACHE_LOCK:
                bc._PREV_DAY_MEMORY_CACHE.clear()
                bc._PREV_DAY_MEMORY_CACHE.update(orig_mem)

        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["Symbol"], "RELIANCE")
        self.assertEqual(df.iloc[0]["Ratio"], 2.5)
        self.assertEqual(df.attrs["provider_meta"]["provider"], "fivepaisa")
        self.assertEqual(df.attrs["provider_meta"]["mode"], "wss")
        self.assertTrue(df.attrs["provider_meta"]["ws_connected"])

    def test_closed_market_empty_intraday_caches_cooldown_without_duplicate_fallback(self):
        today = date(2026, 11, 28)
        tok = _make_fake_jwt({"ClientCode": "50011110", "RedirectServer": "C"})
        prov = fp_mod.FivePaisaProvider(tok, use_live_ws=True, eq_map={"RELIANCE": "2885"})
        fake_stream = fs_mod.FivePaisaMarketStream(tok, "50011110", {"2885": "RELIANCE.NS"})

        with (
            patch.object(fp_mod, "is_market_open", return_value=False),
            patch.object(fp_mod, "get_or_start_fivepaisa_stream", return_value=fake_stream),
            patch.object(prov, "fetch_bars", return_value=({}, set())) as mock_bars,
        ):
            s1, m1 = prov.fetch_today_summaries(["RELIANCE.NS"], today, time(10, 30), "1m")
            s2, m2 = prov.fetch_today_summaries(["RELIANCE.NS"], today, time(10, 31), "1m")

        self.assertEqual(mock_bars.call_count, 1)
        self.assertEqual(s1, {})
        self.assertEqual(s2, {})
        self.assertEqual(m1["mode"], "closed_cached")
        self.assertEqual(m2["mode"], "closed_cached")

    def test_after_market_disables_ws_autostart_and_serves_cached_closing_state(self):
        today = date(2026, 11, 27)
        tok = _make_fake_jwt({"ClientCode": "50011110", "RedirectServer": "C"})
        close_sec = int(h.IST.localize(datetime(2026, 11, 27, 15, 29)).timestamp())
        stream = fs_mod.FivePaisaMarketStream(tok, "50011110", {"2885": "RELIANCE.NS"})
        stream._started = True
        stream._connected = True
        stream._quotes_by_sym["RELIANCE.NS"] = {
            "feed_date": today,
            "ltp": 1540.0,
            "day_open": 1505.0,
            "day_high": 1545.0,
            "day_low": 1500.0,
            "atp": None,
            "vtt": 88000.0,
            "last_vtt": 88000.0,
            "last_ts_sec": close_sec,
            "minute_vols": {},
            "pv_sum": 88000.0 * 1525.0,
            "pv_vol": 88000.0,
        }
        fp_key = fs_mod.secret_fingerprint("50011110", tok)
        with fs_mod._STREAM_LOCK:
            fs_mod._ACTIVE_STREAMS[fp_key] = stream

        prov = fp_mod.FivePaisaProvider(tok, use_live_ws=True, eq_map={"RELIANCE": "2885"})
        with (
            patch.object(fp_mod, "is_market_open", return_value=False),
            patch.object(fp_mod, "is_wss_active_window", return_value=False),
            patch.object(stream, "stop", wraps=stream.stop) as mock_stop,
        ):
            summaries, meta = prov.fetch_today_summaries(["RELIANCE.NS"], today, time(16, 15), "1m")

        self.assertEqual(mock_stop.call_count, 1)
        self.assertFalse(stream._started)
        self.assertEqual(meta["mode"], "post_close_cached")
        self.assertIn("RELIANCE.NS", summaries)
        self.assertEqual(summaries["RELIANCE.NS"][0], 88000.0)

    def test_isolated_baseline_partial_missing_skips_full_refetch_and_guards_non_trading_days(self):
        today = date(2026, 11, 27)
        prev_day = date(2026, 11, 26)
        now_ist = h.IST.localize(datetime(2026, 11, 27, 10, 0))
        tok = _make_fake_jwt({"ClientCode": "50011110", "RedirectServer": "C"})
        prov = fp_mod.FivePaisaProvider(
            tok, use_live_ws=False, eq_map={"RELIANCE": "2885", "ATHERENERG": "757645"}, user_key="UKEY_5P"
        )

        self.assertFalse(prov.supports_coalesced_range)
        self.assertEqual(
            prov._build_candle_url("2885", "1m", "2026-11-26", "2026-11-27"),
            "https://openapi.5paisa.com/V2/historical/N/C/2885/1m?from=2026-11-26&end=2026-11-26",
        )
        self.assertEqual(
            prov._build_candle_url("2885", "1m", "2026-11-26", "2026-11-28"),
            "https://openapi.5paisa.com/V2/historical/N/C/2885/1m?from=2026-11-26&end=2026-11-27",
        )
        self.assertEqual(
            prov._build_candle_url("2885", "5m", "2026-11-26", "2026-11-26"),
            "https://openapi.5paisa.com/V2/historical/N/C/2885/5m?from=2026-11-26&end=2026-11-26",
        )

        # Guards against 5paisa rolling TickDt onto pre-open (<09:15) or closed weekends/holidays
        self.assertTrue(fp_mod._allow_batch_quote_today(date(2026, 11, 27), time(10, 0), True))
        self.assertTrue(fp_mod._allow_batch_quote_today(date(2026, 11, 27), time(16, 15), False))
        self.assertFalse(fp_mod._allow_batch_quote_today(date(2026, 11, 27), time(9, 10), False))
        self.assertFalse(fp_mod._allow_batch_quote_today(date(2026, 11, 28), time(16, 25), False))

        # Baseline has RELIANCE.NS cached, but ATHERENERG.NS is missing -> fetch_bars ONLY fetches ATHERENERG.NS,
        # never re-downloading RELIANCE.NS; today comes from fast V1/MarketFeed Batch Quote.
        cache_key = bc.make_cache_key(prev_day, "1m", "fivepaisa")
        warm_partial_store = {
            "version": bc.BASELINE_FORMAT_VERSION,
            "prev_day": prev_day,
            "interval": "1m",
            "times_sec": np.array([9 * 3600 + 59 * 60], dtype=np.int32),
            "symbols": ["RELIANCE.NS"],
            "symbol_to_idx": {"RELIANCE.NS": 0},
            "cumvol": np.array([[25000.0]], dtype=float),
            "prev_close": np.array([1500.0], dtype=float),
            "prev_full_vol": np.array([90000.0], dtype=float),
            "missing_symbols": set(),
        }
        live_sec = int(h.IST.localize(datetime(2026, 11, 27, 9, 59)).timestamp())
        snaps = [
            {
                "sym": "RELIANCE.NS",
                "feed_date": today,
                "tick_sec": live_sec,
                "ltp": 1540.0,
                "day_open": 1510.0,
                "day_high": 1545.0,
                "day_low": 1505.0,
                "atp": 1530.0,
                "vtt": 75000.0,
            }
        ]
        fetched_syms = []

        def _fake_bars(symbol_periods, **kwargs):
            fetched_syms.extend(sym for sym, _ in symbol_periods)
            return {}, set()

        with bc._CACHE_LOCK:
            orig_mem = dict(bc._PREV_DAY_MEMORY_CACHE)
            orig_neg = dict(bc._NEG_CACHE)
            bc._PREV_DAY_MEMORY_CACHE.clear()
            bc._NEG_CACHE.clear()
            bc._PREV_DAY_MEMORY_CACHE[cache_key] = warm_partial_store

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                with (
                    patch.object(bc, "CACHE_DIR", Path(tmpdir)),
                    patch.object(bc, "_prune_old_cache"),
                    patch.object(scanner, "datetime", wraps=datetime) as mock_dt,
                    patch.object(fp_mod, "is_market_open", return_value=True),
                    patch.object(prov, "fetch_bars", side_effect=_fake_bars),
                    patch.object(prov, "_fetch_batch_quotes_sync", return_value=(snaps, True)) as mock_bq,
                ):
                    mock_dt.now.return_value = now_ist
                    df = scanner.run_scan(
                        ["RELIANCE.NS", "ATHERENERG.NS"],
                        min_ratio=0.0,
                        min_volume=0,
                        interval="1m",
                        prev_day=prev_day,
                        include_oi=False,
                        provider=prov,
                    )
        finally:
            with bc._CACHE_LOCK:
                bc._PREV_DAY_MEMORY_CACHE.clear()
                bc._PREV_DAY_MEMORY_CACHE.update(orig_mem)
                bc._NEG_CACHE.clear()
                bc._NEG_CACHE.update(orig_neg)

        self.assertEqual(fetched_syms, ["ATHERENERG.NS"])
        self.assertEqual(mock_bq.call_count, 1)
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["Symbol"], "RELIANCE")
        self.assertEqual(df.iloc[0]["Ratio"], 3.0)
        self.assertEqual(df.attrs["provider_meta"]["mode"], "batch_quote")

    def test_cold_wss_startup_non_blocking_and_auth_abort_marks_failed(self):
        today = date(2026, 11, 27)
        tok = _make_fake_jwt({"ClientCode": "50011110", "RedirectServer": "C"})

        # 1. Cold WSS at 10:30 (>= 09:20): no sleep, transient empty failure skips duplicate fallback
        prov_open = fp_mod.FivePaisaProvider(tok, use_live_ws=True, eq_map={"RELIANCE": "2885"})
        cold_stream = fs_mod.FivePaisaMarketStream(tok, "50011110", {"2885": "RELIANCE.NS"})
        with (
            patch.object(fp_mod.pytime, "sleep", side_effect=AssertionError("Cold WSS must not sleep")),
            patch.object(fp_mod, "is_market_open", return_value=True),
            patch.object(fp_mod, "get_or_start_fivepaisa_stream", return_value=cold_stream),
            patch.object(prov_open, "fetch_bars", return_value=({}, {"RELIANCE.NS"})) as mock_fail_bars,
        ):
            s_err, m_err = prov_open.fetch_today_summaries(["RELIANCE.NS"], today, time(10, 30), "1m")
        self.assertEqual(mock_fail_bars.call_count, 1)
        self.assertEqual(s_err, {})
        self.assertEqual(m_err["provider"], "fivepaisa")

        # 2. Cold WSS at 09:16 (< 09:20): marks seeded date with zero sleep and falls back cleanly
        prov_early = fp_mod.FivePaisaProvider(tok, use_live_ws=True, eq_map={"RELIANCE": "2885"})
        early_stream = fs_mod.FivePaisaMarketStream(tok, "50011110", {"2885": "RELIANCE.NS"})
        with (
            patch.object(fp_mod.pytime, "sleep", side_effect=AssertionError("Cold WSS must not sleep")),
            patch.object(fp_mod, "is_market_open", return_value=True),
            patch.object(fp_mod, "get_or_start_fivepaisa_stream", return_value=early_stream),
        ):
            s_early, m_early = prov_early.fetch_today_summaries(["RELIANCE.NS"], today, time(9, 16), "1m")
        self.assertIsNone(s_early)
        self.assertEqual(m_early["mode"], "rest_fallback")
        self.assertFalse(early_stream.needs_intraday_seed(today))

        # 3. HTTP 401/403 inside fetch_bars marks unfetched symbols failed to avoid negative-cache poisoning
        prov_auth = fp_mod.FivePaisaProvider(tok, use_live_ws=False, eq_map={"RELIANCE": "2885"})

        def _fake_run_coro(coro_fn):
            coro = coro_fn()
            coro.close()
            prov_auth._set_error("5paisa access token expired", auth_failed=True)
            return {}, set()

        with patch.object(fp_mod.yahoo_client, "_run_coro_sync", side_effect=_fake_run_coro):
            out, failed = prov_auth.fetch_bars([("RELIANCE.NS", "1d")], interval="1m")
        self.assertEqual(out, {})
        self.assertEqual(failed, {"RELIANCE.NS"})
        self.assertTrue(prov_auth._is_auth_failed())

    def test_batch_market_feed_parser_chunking_rest_mode_and_closed_cooldown(self):
        import types

        today = date(2026, 11, 27)
        live_ms = int(h.IST.localize(datetime(2026, 11, 27, 10, 15, 0)).timestamp() * 1000)
        stale_ms = int(h.IST.localize(datetime(2026, 11, 26, 15, 29, 0)).timestamp() * 1000)
        code_to_sym = {"2885": "RELIANCE.NS", "11536": "TCS.NS", "1660": "ITC.NS"}

        # 1. Parser accepts MarketFeedData/Data entries for today >= 09:15 and rejects stale/zero/error payloads
        payload = {
            "head": {"responseCode": "5PMFV1", "status": "0", "statusDescription": "Success"},
            "body": {
                "Status": 0,
                "Message": "Success",
                "MarketFeedData": [
                    {
                        "Token": 2885,
                        "Symbol": "RELIANCE",
                        "LastRate": 1525.5,
                        "Open": 1500.0,
                        "High": 1530.0,
                        "Low": 1495.0,
                        "AverageRate": 1518.2,
                        "TotalQty": 90000,
                        "LastTradedTime": f"/Date({live_ms}+0530)/",
                    },
                    {
                        "Token": 11536,
                        "Symbol": "TCS",
                        "LastRate": 4000.0,
                        "TotalQty": 50000,
                        "TickDt": f"/Date({stale_ms}+0530)/",
                    },
                    {
                        "Token": 1660,
                        "Symbol": "ITC",
                        "LastRate": 440.0,
                        "TotalQty": 0,
                        "TickDt": f"/Date({live_ms}+0530)/",
                    },
                ],
            },
        }
        snaps, ok = fp_mod._parse_fivepaisa_market_feed(payload, code_to_sym, today)
        self.assertTrue(ok)
        self.assertEqual(len(snaps), 1)
        self.assertEqual(snaps[0]["sym"], "RELIANCE.NS")
        self.assertEqual(snaps[0]["vtt"], 90000.0)
        self.assertAlmostEqual(snaps[0]["atp"], 1518.2)

        bad_snaps, bad_ok = fp_mod._parse_fivepaisa_market_feed(
            {"head": {"status": "2"}, "body": {"Status": 1}}, code_to_sym, today
        )
        self.assertEqual(bad_snaps, [])
        self.assertFalse(bad_ok)

        # 2. Batch chunking splits >50 scrips into <=50 POST chunks and retries 429 cleanly
        tok = _make_fake_jwt({"ClientCode": "50011110", "RedirectServer": "C"})
        many_eq = {f"SYM{i}": str(1000 + i) for i in range(55)}
        many_s2c = {f"{k}.NS": v for k, v in many_eq.items()}
        many_c2s = {v: f"{k}.NS" for k, v in many_eq.items()}
        post_calls = []

        class _FakeResp:
            def __init__(self, status_code, body_payload):
                self.status_code = status_code
                self._payload = body_payload
                self.content = json.dumps(body_payload).encode("utf-8")

            def json(self):
                return self._payload

        class _FakeSess:
            def __init__(self, impersonate="chrome"):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

            async def post(self, url, headers=None, json=None, timeout=10):
                post_calls.append((url, headers, json))
                items = json["body"]["MarketFeedData"]
                if len(post_calls) == 1:
                    return _FakeResp(429, {})
                return _FakeResp(
                    200,
                    {
                        "head": {"status": "0"},
                        "body": {
                            "Status": 0,
                            "Data": [
                                {
                                    "ScripCode": item["ScripCode"],
                                    "LastRate": 100.0,
                                    "OpenRate": 99.0,
                                    "High": 101.0,
                                    "Low": 98.5,
                                    "TotalQty": 2500,
                                    "TickDt": f"/Date({live_ms}+0530)/",
                                }
                                for item in items
                            ],
                        },
                    },
                )

        curl_mod = types.ModuleType("curl_cffi")
        curl_req = types.ModuleType("curl_cffi.requests")
        curl_req.AsyncSession = _FakeSess
        curl_mod.requests = curl_req
        prov_chunk = fp_mod.FivePaisaProvider(tok, use_live_ws=False, eq_map=many_eq, user_key="UKEY_5P")

        def _direct_coro(coro_fn):
            import asyncio as _asyncio

            return _asyncio.run(coro_fn())

        with (
            patch.dict(sys.modules, {"curl_cffi": curl_mod, "curl_cffi.requests": curl_req}),
            patch("providers.common.http_runtime.yahoo_client._ASYNC_LOOP", new=None),
            patch.object(fp_mod.yahoo_client, "_run_coro_sync", side_effect=_direct_coro),
        ):
            chunk_snaps, chunk_ok = prov_chunk._fetch_batch_quotes_sync(many_s2c, many_c2s, today)

        self.assertTrue(chunk_ok)
        self.assertEqual(len(chunk_snaps), 55)
        self.assertEqual(len(post_calls), 3)  # 2 initial chunks (50 + 5) + 1 retry for HTTP 429
        self.assertEqual(post_calls[0][0], fp_mod._FIVEPAISA_BATCH_FEED_URL)
        self.assertEqual(post_calls[0][2]["head"]["Key"], "UKEY_5P")
        batch_sizes = sorted(len(c[2]["body"]["MarketFeedData"]) for c in post_calls)
        self.assertEqual(batch_sizes, [5, 50, 50])

        # 3. Pure REST mode & Cold WSS mode use batch quote (~0.25s) + non-blocking 1m seed; closed enters cooldown
        prov_rest = fp_mod.FivePaisaProvider(
            tok, use_live_ws=False, eq_map={"RELIANCE": "2885", "TCS": "11536"}, user_key="UKEY_5P"
        )
        with (
            patch.object(fp_mod, "is_market_open", return_value=True),
            patch.object(prov_rest, "_fetch_batch_quotes_sync", return_value=(snaps, True)) as mock_bq,
            patch.object(prov_rest, "fetch_bars", side_effect=AssertionError("REST batch quote skips fetch_bars")),
        ):
            summaries, meta = prov_rest.fetch_today_summaries(["RELIANCE.NS", "TCS.NS"], today, time(10, 20), "1m")
        self.assertEqual(mock_bq.call_count, 1)
        self.assertEqual(meta["mode"], "batch_quote")
        self.assertEqual(summaries["RELIANCE.NS"][0], 90000.0)
        self.assertAlmostEqual(summaries["RELIANCE.NS"][6], 1518.2)

        prov_wss_cold = fp_mod.FivePaisaProvider(
            tok, use_live_ws=True, eq_map={"RELIANCE": "2885"}, user_key="UKEY_5P"
        )
        cold_stream = fs_mod.FivePaisaMarketStream(tok, "50011110", {"2885": "RELIANCE.NS"})
        with (
            patch.object(fp_mod.pytime, "sleep", side_effect=AssertionError("Must not sleep")),
            patch.object(fp_mod, "is_market_open", return_value=True),
            patch.object(fp_mod, "get_or_start_fivepaisa_stream", return_value=cold_stream),
            patch.object(prov_wss_cold, "_fetch_batch_quotes_sync", return_value=(snaps, True)) as mock_wss_bq,
            patch.object(prov_wss_cold, "_start_background_intraday_seed") as mock_bg_seed,
        ):
            wss_s, wss_m = prov_wss_cold.fetch_today_summaries(["RELIANCE.NS"], today, time(10, 20), "1m")
        mock_wss_bq.assert_called_once()
        mock_bg_seed.assert_called_once()
        self.assertEqual(wss_m["mode"], "batch_quote")
        self.assertEqual(wss_s["RELIANCE.NS"][0], 90000.0)

        prov_closed = fp_mod.FivePaisaProvider(tok, use_live_ws=False, eq_map={"RELIANCE": "2885"}, user_key="UKEY_5P")
        with (
            patch.object(fp_mod, "is_market_open", return_value=False),
            patch.object(prov_closed, "_fetch_batch_quotes_sync", return_value=([], True)) as mock_cl_bq,
        ):
            s1, m1 = prov_closed.fetch_today_summaries(["RELIANCE.NS"], today, time(10, 20), "1m")
            s2, m2 = prov_closed.fetch_today_summaries(["RELIANCE.NS"], today, time(10, 21), "1m")
        self.assertEqual(mock_cl_bq.call_count, 1)
        self.assertEqual(s1, {})
        self.assertEqual(s2, {})
        self.assertEqual(m1["mode"], "closed_cached")
        self.assertEqual(m2["mode"], "closed_cached")


if __name__ == "__main__":
    unittest.main(verbosity=2)
