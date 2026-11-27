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

import baseline_cache as bc
import holidays as h
import providers.upstox_instruments as ui_mod
import providers.upstox_provider as up_mod
import providers.upstox_stream as us_mod
import scanner


class UpstoxProviderTest(unittest.TestCase):
    def setUp(self):
        with ui_mod._LOCK:
            ui_mod._MEM_CACHE = None
            ui_mod._MEM_CACHE_DATE = None
        us_mod.stop_upstox_stream()

    def tearDown(self):
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


if __name__ == "__main__":
    unittest.main(verbosity=2)
