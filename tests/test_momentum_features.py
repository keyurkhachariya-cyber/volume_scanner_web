import os
import sys
import unittest
from datetime import date, datetime, time
from unittest.mock import patch

import numpy as np

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import core.baseline_cache as bc
import core.holidays as h
import core.scanner as s
import providers.yahoo.client as yc


class MomentumFeaturesTest(unittest.TestCase):
    def test_summarize_today_arrays_vwap_and_vol_burst_15m(self):
        today = date(2026, 11, 27)
        day_start = s._ist_epoch_sec(today, time(0, 0))
        open_sec = day_start + 9 * 3600 + 15 * 60

        # 30 bars (09:15 .. 09:44): first 15 bars low volume (100/bar), last 15 bars surge (500/bar)
        # First 15 TP = 100.0 (total vol 1500), last 15 TP = 110.0 (total vol 7500)
        # Expected VWAP = (1500*100 + 7500*110) / 9000 = 108.3333...
        # Total vol = 9000 over 30m -> avg 300/min; last 15m vol = 7500 -> 500/min => burst = 500/300 = 1.6667x
        ts = np.array([open_sec + i * 60 for i in range(30)], dtype=np.int64)
        op = np.array([100.0] * 15 + [110.0] * 15, dtype=float)
        hi = np.array([101.0] * 15 + [111.0] * 15, dtype=float)
        lo = np.array([99.0] * 15 + [109.0] * 15, dtype=float)
        cl = np.array([100.0] * 15 + [110.0] * 15, dtype=float)
        vol = np.array([100.0] * 15 + [500.0] * 15, dtype=float)

        summary = s._summarize_today_arrays(
            ts, op, hi, lo, cl, vol, today, time(9, 45), day_start, day_start + 9 * 3600 + 45 * 60, 60
        )
        self.assertIsNotNone(summary)
        vol_today, ltp, d_open, d_high, d_low, last_bar, vwap, burst = summary
        self.assertAlmostEqual(vol_today, 9000.0)
        self.assertAlmostEqual(ltp, 110.0)
        self.assertAlmostEqual(d_open, 100.0)
        self.assertAlmostEqual(d_high, 111.0)
        self.assertAlmostEqual(d_low, 99.0)
        self.assertEqual(last_bar, time(9, 44))
        self.assertAlmostEqual(vwap, (1500 * 100.0 + 7500 * 110.0) / 9000.0, places=4)
        self.assertAlmostEqual(burst, 5.0 / 3.0, places=4)

        # Duplicate timestamp deduped for both VWAP & burst
        ts_dup = np.insert(ts, 15, ts[15])
        op_dup = np.insert(op, 15, op[15])
        hi_dup = np.insert(hi, 15, hi[15])
        lo_dup = np.insert(lo, 15, lo[15])
        cl_dup = np.insert(cl, 15, cl[15])
        vol_dup = np.insert(vol, 15, 250.0)
        summary_dup = s._summarize_today_arrays(
            ts_dup, op_dup, hi_dup, lo_dup, cl_dup, vol_dup, today, time(9, 45), day_start, day_start + 9 * 3600 + 45 * 60, 60
        )
        self.assertAlmostEqual(summary_dup[0], 9000.0)
        self.assertAlmostEqual(summary_dup[7], 5.0 / 3.0, places=4)

        # Early-day session <= 15m yields vol_burst_15m = None
        summary_early = s._summarize_today_arrays(
            ts[:10], op[:10], hi[:10], lo[:10], cl[:10], vol[:10], today, time(9, 30), day_start, day_start + 9 * 3600 + 30 * 60, 60
        )
        self.assertIsNone(summary_early[7])

        # Yahoo NSE closing NaN gap (15:16..15:28 nan before 15:29 bar) still yields accurate active-bar burst
        full_open = day_start + 9 * 3600 + 15 * 60
        ts_full = np.array([full_open + i * 60 for i in range(375)], dtype=np.int64)
        op_full = np.full(375, 100.0)
        hi_full = np.full(375, 101.0)
        lo_full = np.full(375, 99.0)
        cl_full = np.full(375, 100.0)
        vol_full = np.full(375, 100.0)
        # First 347 bars: 100, last 14 before gap (347..360): 300, gap 361..373 (15:16..15:28): nan, bar 374 (15:29): 300
        vol_full[347:361] = 300.0
        op_full[361:374] = np.nan
        hi_full[361:374] = np.nan
        lo_full[361:374] = np.nan
        cl_full[361:374] = np.nan
        vol_full[361:374] = np.nan
        vol_full[374] = 300.0
        summary_gap = s._summarize_today_arrays(
            ts_full, op_full, hi_full, lo_full, cl_full, vol_full, today, time(15, 35), day_start, day_start + 15 * 3600 + 35 * 60, 60
        )
        # Last 15 active bars all have vol=300; overall avg is ~108.3 across 362 bars -> burst > 2.5x (NOT ~0.07x!)
        self.assertGreater(summary_gap[7], 2.5)

    def test_run_scan_attaches_new_momentum_columns(self):
        today = date(2026, 11, 27)
        prev_day = date(2026, 11, 26)
        now_ist = datetime(2026, 11, 27, 10, 0, tzinfo=h.IST)
        day_start = s._ist_epoch_sec(today, time(0, 0))
        open_sec = day_start + 9 * 3600 + 15 * 60
        cache_key = (prev_day, "1m")

        ts = np.array([open_sec + i * 60 for i in range(45)], dtype=np.int64)
        # Strong intraday runner: opens 102 (prev_close=100 -> Gap +2%), climbs to high 108, closes 107.5
        op = np.full(45, 102.0)
        hi = np.linspace(102.5, 108.0, 45)
        lo = np.full(45, 101.0)
        cl = np.linspace(102.0, 107.5, 45)
        vol = np.array([1000.0] * 30 + [4000.0] * 15)

        fake_store = {
            "version": s.BASELINE_FORMAT_VERSION,
            "prev_day": prev_day,
            "interval": "1m",
            "times_sec": np.array([9 * 3600 + 59 * 60], dtype=np.int64),
            "symbols": ["RUNNER.NS"],
            "symbol_to_idx": {"RUNNER.NS": 0},
            "cumvol": np.array([[20000.0]], dtype=float),
            "prev_close": np.array([100.0], dtype=float),
            "prev_full_vol": np.array([50000.0], dtype=float),
            "missing_symbols": set(),
        }
        bundles = {"RUNNER.NS": {"ts_sec": ts, "open": op, "high": hi, "low": lo, "close": cl, "volume": vol}}

        with bc._CACHE_LOCK:
            orig = dict(bc._PREV_DAY_MEMORY_CACHE)
            bc._PREV_DAY_MEMORY_CACHE.clear()
            bc._PREV_DAY_MEMORY_CACHE[cache_key] = fake_store
        try:
            with (
                patch.object(s, "datetime", wraps=datetime) as mock_dt,
                patch.object(s, "get_previous_trading_day", return_value=prev_day),
                patch.object(bc, "_prune_old_cache"),
                patch.object(yc, "_fetch_market_data", return_value=(bundles, set())),
            ):
                mock_dt.now.return_value = now_ist
                df = s.run_scan(["RUNNER.NS"], min_ratio=0.0, min_volume=0, interval="1m", prev_day=prev_day, include_oi=False)
        finally:
            with bc._CACHE_LOCK:
                bc._PREV_DAY_MEMORY_CACHE.clear()
                bc._PREV_DAY_MEMORY_CACHE.update(orig)

        row = df.iloc[0]
        self.assertGreater(row["VWAP_Dist_Pct"], 0.0)
        self.assertAlmostEqual(row["Range_Pos_Pct"], ((107.5 - 101.0) / 7.0) * 100.0, delta=0.2)
        self.assertAlmostEqual(row["From_Open_Pct"], ((107.5 - 102.0) / 102.0) * 100.0, places=2)
        self.assertGreater(row["Vol_Burst_15m"], 1.5)


if __name__ == "__main__":
    unittest.main(verbosity=2)
