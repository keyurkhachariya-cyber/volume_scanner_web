import os
import sys
import unittest
from unittest.mock import patch

import pandas as pd
from streamlit.testing.v1 import AppTest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import scanner
import universe


def _build_sample_df(include_buildup: bool = True) -> pd.DataFrame:
    rows = [
        {
            "Symbol": "RELIANCE",
            "LTP": 1500.0,
            "Vol_Today": 5000000,
            "Vol_Prev_Same_Time": 2000000,
            "Ratio": 2.5,
            "Pct_Change": 2.1,
            "Last_Bar_Time": "14:05",
            "Gap_Pct": 0.2,
            "Prev_Close": 1469.0,
            "Day_Open": 1472.0,
            "Day_High": 1505.0,
            "Day_Low": 1468.0,
            "Range_Pct": 2.52,
            "Vol_Diff": 3000000,
            "Prev_Full_Vol": 8000000,
            "Turnover_Cr": 750.0,
            "OI": 1200,
            "OI_Prev": 1000,
            "OI_Chg": 200,
            "OI_Chg_Pct": 20.0,
            "Vol_Burst_15m": 1.85,
            "From_Open_Pct": 1.9,
            "VWAP_Dist_Pct": 0.95,
            "Range_Pos_Pct": 92.0,
            "VWAP": 1485.8,
        },
        {
            "Symbol": "HDFCBANK",
            "LTP": 1700.0,
            "Vol_Today": 4000000,
            "Vol_Prev_Same_Time": 1000000,
            "Ratio": 4.0,
            "Pct_Change": -1.8,
            "Last_Bar_Time": "14:05",
            "Gap_Pct": -0.1,
            "Prev_Close": 1731.0,
            "Day_Open": 1729.0,
            "Day_High": 1732.0,
            "Day_Low": 1695.0,
            "Range_Pct": 2.18,
            "Vol_Diff": 3000000,
            "Prev_Full_Vol": 6000000,
            "Turnover_Cr": 680.0,
            "OI": 750,
            "OI_Prev": 1000,
            "OI_Chg": -250,
            "OI_Chg_Pct": -25.0,
            "Vol_Burst_15m": 1.5,
            "From_Open_Pct": -1.68,
            "VWAP_Dist_Pct": -0.8,
            "Range_Pos_Pct": 13.5,
            "VWAP": 1713.7,
        },
        {
            "Symbol": "INFY",
            "LTP": 1850.0,
            "Vol_Today": 3000000,
            "Vol_Prev_Same_Time": 1500000,
            "Ratio": 2.0,
            "Pct_Change": 1.2,
            "Last_Bar_Time": "14:05",
            "Gap_Pct": 0.1,
            "Prev_Close": 1828.0,
            "Day_Open": 1860.0,  # Gap faded: +1.2% vs prev close, but -0.54% below Day Open!
            "Day_High": 1875.0,
            "Day_Low": 1825.0,
            "Range_Pct": 2.74,
            "Vol_Diff": 1500000,
            "Prev_Full_Vol": 5000000,
            "Turnover_Cr": 555.0,
            "OI": 900,
            "OI_Prev": 1000,
            "OI_Chg": -100,
            "OI_Chg_Pct": -10.0,
            "Vol_Burst_15m": 0.4,  # Stale morning spike
            "From_Open_Pct": -0.54,
            "VWAP_Dist_Pct": -0.3,
            "Range_Pos_Pct": 50.0,
            "VWAP": 1855.5,
        },
        {
            "Symbol": "TCS",
            "LTP": 4000.0,
            "Vol_Today": 2000000,
            "Vol_Prev_Same_Time": 1000000,
            "Ratio": 2.0,
            "Pct_Change": -1.0,
            "Last_Bar_Time": "14:05",
            "Gap_Pct": 0.0,
            "Prev_Close": 4040.0,
            "Day_Open": 4040.0,
            "Day_High": 4050.0,
            "Day_Low": 3990.0,
            "Range_Pct": 1.5,
            "Vol_Diff": 1000000,
            "Prev_Full_Vol": 4000000,
            "Turnover_Cr": 800.0,
            "OI": 1040,
            "OI_Prev": 1000,
            "OI_Chg": 40,
            "OI_Chg_Pct": 4.0,
            "Vol_Burst_15m": 0.7,
            "From_Open_Pct": -0.99,
            "VWAP_Dist_Pct": -0.4,
            "Range_Pos_Pct": 16.7,
            "VWAP": 4016.0,
        },
        {
            "Symbol": "SBIN",
            "LTP": 780.0,
            "Vol_Today": 6000000,
            "Vol_Prev_Same_Time": 3333333,
            "Ratio": 1.8,
            "Pct_Change": -2.2,
            "Last_Bar_Time": "14:05",
            "Gap_Pct": -0.2,
            "Prev_Close": 797.5,
            "Day_Open": 796.0,
            "Day_High": 798.0,
            "Day_Low": 779.0,
            "Range_Pct": 2.44,
            "Vol_Diff": 2666667,
            "Prev_Full_Vol": 9000000,
            "Turnover_Cr": 468.0,
            "OI": 1140,
            "OI_Prev": 1000,
            "OI_Chg": 140,
            "OI_Chg_Pct": 14.0,
            "Vol_Burst_15m": 1.6,
            "From_Open_Pct": -2.01,
            "VWAP_Dist_Pct": -1.1,
            "Range_Pos_Pct": 5.3,
            "VWAP": 788.7,
        },
        {
            "Symbol": "NONFNO",
            "LTP": 500.0,
            "Vol_Today": 1500000,
            "Vol_Prev_Same_Time": 500000,
            "Ratio": 3.0,
            "Pct_Change": 0.9,
            "Last_Bar_Time": "14:05",
            "Gap_Pct": 0.0,
            "Prev_Close": 495.5,
            "Day_Open": 495.5,
            "Day_High": 502.0,
            "Day_Low": 495.0,
            "Range_Pct": 1.41,
            "Vol_Diff": 1000000,
            "Prev_Full_Vol": 2000000,
            "Turnover_Cr": 75.0,
            "OI": None,
            "OI_Prev": None,
            "OI_Chg": None,
            "OI_Chg_Pct": None,
            "Vol_Burst_15m": 1.1,
            "From_Open_Pct": 0.91,
            "VWAP_Dist_Pct": 0.25,
            "Range_Pos_Pct": 71.4,
            "VWAP": 498.75,
        },
    ]
    if include_buildup:
        tags = ["Long Buildup", "Long Unwinding", "Short Covering", "Short Buildup", "Short Buildup", None]
        sigs = ["🟢 Bullish", "—", "—", "—", "🔴 Bearish", None]
        for row, tag, sig in zip(rows, tags, sigs):
            row["OI_Buildup"] = tag
            if sig is not None:
                row["Signal"] = sig
            else:
                row["Signal"] = "—"
    df = pd.DataFrame(rows)
    df.attrs["oi_meta"] = {
        "nse_timestamp": "26-Nov-2026 14:05:00",
        "from_cache": False,
        "cooldown_seconds": 0.0,
        "error": None,
    }
    return df


class AppOiUiTest(unittest.TestCase):
    def test_default_columns_oi_filters_buildup_and_sorting(self):
        sample_df = _build_sample_df(include_buildup=True)
        at = AppTest.from_file(os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")), "app.py"), default_timeout=15)

        with (
            patch.object(scanner, "run_scan", return_value=sample_df) as mock_scan,
            patch.object(universe, "get_fno_universe", return_value=(["RELIANCE.NS", "HDFCBANK.NS"], "Mock")),
        ):
            at.run()
            scan_btn = [b for b in at.button if "Scan Now" in b.label][0]
            scan_btn.click().run()
            self.assertEqual(mock_scan.call_count, 1)
            self.assertEqual(list(at.exception), [])
            rendered_df = at.dataframe[0].value
            self.assertEqual(
                list(rendered_df.columns),
                [
                    "Signal",
                    "Symbol",
                    "LTP",
                    "Ratio",
                    "Vol_Burst_15m",
                    "Pct_Change",
                    "From_Open_Pct",
                    "VWAP_Dist_Pct",
                    "Range_Pos_Pct",
                    "OI_Chg_Pct",
                    "OI_Buildup",
                ],
            )
            import table_config

            for hidden_col in ("Vol_Today", "Vol_Prev_Same_Time", "OI", "OI_Chg", "Last_Bar_Time"):
                self.assertNotIn(hidden_col, rendered_df.columns)
                self.assertIn(hidden_col, table_config.ALL_COLUMNS)
            for removed_col in ("OI_Prev", "VWAP", "Prev_Close", "Day_Open", "Day_High", "Day_Low", "Vol_Diff", "Prev_Full_Vol"):
                self.assertNotIn(removed_col, table_config.ALL_COLUMNS)
            self.assertEqual(len(table_config.ALL_COLUMNS), 19)
            self.assertEqual(list(rendered_df["Symbol"]), ["HDFCBANK", "NONFNO", "RELIANCE", "INFY", "TCS", "SBIN"])
            self.assertTrue(any("🟢 Bullish" in m.value and "**1**" in m.value for m in at.markdown))

            # Enable Momentum Quality Filters -> filters out INFY (red candle/below VWAP/low burst), TCS (low burst), NONFNO (burst 1.1 < 1.2)
            mom_chk = [c for c in at.checkbox if c.label == "Momentum Quality Filters"][0]
            mom_chk.set_value(True).run()
            self.assertEqual(mock_scan.call_count, 1)  # Pure in-memory filter
            self.assertEqual(set(at.dataframe[0].value["Symbol"]), {"RELIANCE", "HDFCBANK", "SBIN"})
            mom_chk = [c for c in at.checkbox if c.label == "Momentum Quality Filters"][0]
            mom_chk.set_value(False).run()

            # Sort by Signal (Bullish / Bearish first) -> RELIANCE (🟢) then SBIN (🔴) at top
            sort_sb = [s for s in at.selectbox if s.label == "Sort results by"][0]
            sort_sb.set_value("Signal (Bullish / Bearish first)").run()
            self.assertEqual(list(at.dataframe[0].value["Symbol"])[:2], ["RELIANCE", "SBIN"])

            # Quick-toggle segmented_control to 🔴 Bearish -> only SBIN and status count stays synced
            sig_pills = [c for c in at.segmented_control if c.label == "Quick Signal Filter"][0]
            sig_pills.set_value("🔴 Bearish").run()
            self.assertEqual(mock_scan.call_count, 1)
            self.assertEqual(list(at.dataframe[0].value["Symbol"]), ["SBIN"])
            self.assertTrue(any("Showing **1** 🔴 Bearish of **6** matching" in s.value for s in at.success))
            [c for c in at.segmented_control if c.label == "Quick Signal Filter"][0].set_value("All").run()

            # Sort by largest |OI Chg %| Day moves -> HDFCBANK (-25%) then RELIANCE (+20%) then SBIN (+14%)
            sort_sb = [s for s in at.selectbox if s.label == "Sort results by"][0]
            sort_sb.set_value("|OI Chg %| Day (Largest Moves)").run()
            self.assertEqual(mock_scan.call_count, 1)  # Pure in-memory sort, no re-scan
            rendered_sorted = at.dataframe[0].value
            self.assertEqual(
                list(rendered_sorted["Symbol"]),
                ["HDFCBANK", "RELIANCE", "SBIN", "INFY", "TCS", "NONFNO"],
            )

            # Filter by Min OI Chg % (Day) = 5.0 -> RELIANCE (20.0) & SBIN (14.0) pass
            oi_min_input = [n for n in at.number_input if n.label == "Min OI Chg % (Day)"][0]
            oi_min_input.set_value(5.0).run()
            self.assertEqual(set(at.dataframe[0].value["Symbol"]), {"RELIANCE", "SBIN"})

            # Clear Min OI Chg % and filter by OI Buildup -> Long Buildup & Short Covering
            oi_min_input = [n for n in at.number_input if n.label == "Min OI Chg % (Day)"][0]
            oi_min_input.set_value(None).run()
            buildup_ms = [m for m in at.multiselect if m.label == "OI Buildup (Price Δ vs OI Δ)"][0]
            buildup_ms.set_value(["Long Buildup", "Short Covering"]).run()
            self.assertEqual(set(at.dataframe[0].value["Symbol"]), {"RELIANCE", "INFY"})

            # Uncheck Include NSE OI -> OI & Signal controls disappear cleanly without re-scanning, Ratio sort applies
            oi_chk = [c for c in at.checkbox if c.label == "Include NSE OI"][0]
            oi_chk.set_value(False).run()
            self.assertEqual(mock_scan.call_count, 1)
            self.assertTrue(at.session_state._last_filter_state[6] is False)
            self.assertEqual(list(at.exception), [])
            self.assertFalse(any(n.label == "Min OI Chg % (Day)" for n in at.number_input))

    def test_momentum_filter_allows_flat_prev_close_with_intraday_recovery(self):
        from screener_filters import filter_and_sort_results
        from table_config import SORT_OPTIONS

        df = pd.DataFrame(
            [
                {
                    "Symbol": "FLATREC",
                    "Ratio": 2.2,
                    "Vol_Today": 120000,
                    "Pct_Change": 0.0,
                    "From_Open_Pct": 1.8,
                    "VWAP_Dist_Pct": 0.6,
                    "Range_Pos_Pct": 91.0,
                    "Vol_Burst_15m": 1.5,
                },
                {
                    "Symbol": "DEADFLAT",
                    "Ratio": 2.0,
                    "Vol_Today": 120000,
                    "Pct_Change": 0.0,
                    "From_Open_Pct": 0.0,
                    "VWAP_Dist_Pct": 0.0,
                    "Range_Pos_Pct": 50.0,
                    "Vol_Burst_15m": 1.5,
                },
            ]
        )
        out = filter_and_sort_results(
            raw_df=df,
            min_ratio=1.5,
            min_volume=50000,
            exclude_gap_up=False,
            exclude_gap_down=False,
            gap_threshold=0.5,
            include_oi=False,
            min_oi_chg_pct=None,
            oi_buildup_filter=(),
            sort_by="Ratio (High → Low)",
            sort_options=SORT_OPTIONS,
            momentum_filter_state=(True, True, 30, 1.2),
        )[0]
        self.assertEqual(list(out["Symbol"]), ["FLATREC"])

    def test_sort_by_oi_chg_lots_and_lazy_signal_and_buildup_backfill(self):
        stale_df = _build_sample_df(include_buildup=False)
        at = AppTest.from_file(os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")), "app.py"), default_timeout=15)

        with (
            patch.object(scanner, "run_scan", return_value=stale_df),
            patch.object(universe, "get_fno_universe", return_value=(["RELIANCE.NS"], "Mock")),
        ):
            at.run()
            [b for b in at.button if "Scan Now" in b.label][0].click().run()
            self.assertIn("Signal", at.dataframe[0].value.columns)
            [c for c in at.segmented_control if c.label == "Quick Signal Filter"][0].set_value("🟢 Bullish").run()
            self.assertEqual(list(at.dataframe[0].value["Symbol"]), ["RELIANCE"])
            [c for c in at.segmented_control if c.label == "Quick Signal Filter"][0].set_value("All").run()
            sort_sb = [s for s in at.selectbox if s.label == "Sort results by"][0]
            sort_sb.set_value("OI Chg Lots Day (High → Low)").run()
            self.assertEqual(list(at.exception), [])
            buildup_ms = [m for m in at.multiselect if m.label == "OI Buildup (Price Δ vs OI Δ)"][0]
            buildup_ms.set_value(["Short Covering"]).run()
            self.assertEqual(list(at.dataframe[0].value["Symbol"]), ["INFY"])

    def test_upstox_data_source_sidebar_controls(self):
        at = AppTest.from_file(os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")), "app.py"), default_timeout=15)
        with patch.object(universe, "get_fno_universe", return_value=(["RELIANCE.NS"], "Mock")):
            at.run()
            src_ctrl = [c for c in at.segmented_control if c.label == "Data Source"][0]
            src_ctrl.set_value("Upstox v3").run()
            self.assertEqual(list(at.exception), [])
            self.assertTrue(any(t.label == "Upstox Access Token (Daily)" for t in at.text_input))
            self.assertTrue(any(c.label.startswith("Stream Live WSS") for c in at.checkbox))


if __name__ == "__main__":
    unittest.main(verbosity=2)
