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
        with fp_mod._SEED_LOCK:
            fp_mod._INFLIGHT_SEEDS.clear()
            fp_mod._EMPTY_TODAY_UNTIL.clear()
        fs_mod.stop_fivepaisa_stream()

    def tearDown(self):
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


if __name__ == "__main__":
    unittest.main(verbosity=2)
