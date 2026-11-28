import base64
from datetime import date
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd
from streamlit.testing.v1 import AppTest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import core.scanner as scanner
import core.universe as universe


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
            import ui.table_config as table_config

            for hidden_col in ("Vol_Today", "Vol_Prev_Same_Time", "OI", "OI_Chg", "Last_Bar_Time", "Chart"):
                self.assertNotIn(hidden_col, rendered_df.columns)
                self.assertIn(hidden_col, table_config.ALL_COLUMNS)
            for removed_col in ("OI_Prev", "VWAP", "Prev_Close", "Day_Open", "Day_High", "Day_Low", "Vol_Diff", "Prev_Full_Vol"):
                self.assertNotIn(removed_col, table_config.ALL_COLUMNS)
            self.assertEqual(len(table_config.ALL_COLUMNS), 20)
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
        from ui.screener_filters import filter_and_sort_results
        from ui.table_config import SORT_OPTIONS

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

    def test_upstox_and_fivepaisa_data_source_sidebar_controls(self):
        at = AppTest.from_file(os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")), "app.py"), default_timeout=15)
        with patch.object(universe, "get_fno_universe", return_value=(["RELIANCE.NS"], "Mock")):
            at.run()
            src_ctrl = [c for c in at.segmented_control if c.label == "Data Source"][0]
            src_ctrl.set_value("Upstox v3").run()
            self.assertEqual(list(at.exception), [])
            self.assertTrue(any(t.label == "Upstox Access Token (Daily)" for t in at.text_input))
            self.assertTrue(any(c.label.startswith("Stream Live WSS") for c in at.checkbox))
            self.assertFalse(any(s.label == "Parallel download threads" for s in at.slider))

            src_ctrl = [c for c in at.segmented_control if c.label == "Data Source"][0]
            src_ctrl.set_value("5paisa Xstream").run()
            self.assertEqual(list(at.exception), [])
            self.assertTrue(any(t.label == "5paisa Access Token (Daily)" for t in at.text_input))
            self.assertTrue(any(t.label == "5paisa Client Code" for t in at.text_input))
            self.assertTrue(any(c.label == "Stream Live WSS (MarketFeedV3)" for c in at.checkbox))
            self.assertTrue(any(b.label == "Login via TOTP & Save Token" for b in at.button))

    def test_empty_state_card_and_optional_chart_link_column(self):
        sample_df = _build_sample_df(include_buildup=True)
        at = AppTest.from_file(os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")), "app.py"), default_timeout=15)
        with (
            patch.object(scanner, "run_scan", return_value=sample_df),
            patch.object(universe, "get_fno_universe", return_value=(["RELIANCE.NS"], "Mock")),
        ):
            at.run()
            self.assertTrue(any("Ready to scan" in m.value for m in at.markdown))
            [b for b in at.button if "Scan Now" in b.label][0].click().run()
            col_ms = [m for m in at.multiselect if m.label.startswith("Visible columns")][0]
            col_ms.set_value(["Symbol", "Chart", "Range_Pos_Pct"]).run()
            self.assertEqual(list(at.exception), [])
            rendered = at.dataframe[0].value
            self.assertEqual(list(rendered.columns), ["Symbol", "Chart", "Range_Pos_Pct"])
            self.assertTrue(all(str(u).startswith("https://in.tradingview.com/chart/?symbol=NSE%3A") for u in rendered["Chart"]))

    def test_auth_helpers_extract_oauth_param_and_jwt_status(self):
        from ui.auth_helpers import extract_oauth_param, inspect_fivepaisa_jwt

        self.assertEqual(
            extract_oauth_param("https://my-app.streamlit.app/?code=AUTH123&state=xyz", ("code",)),
            "AUTH123",
        )
        self.assertEqual(
            extract_oauth_param("?requesttoken=REQ999&clientcode=500111", ("RequestToken", "requesttoken")),
            "REQ999",
        )
        self.assertEqual(extract_oauth_param("  BARE_CODE  ", ("code",)), "BARE_CODE")

        def _make_jwt(claims: dict) -> str:
            hdr = base64.urlsafe_b64encode(b'{"alg":"HS256"}').decode("ascii").rstrip("=")
            body = base64.urlsafe_b64encode(json.dumps(claims).encode("utf-8")).decode("ascii").rstrip("=")
            return f"{hdr}.{body}.sig"

        valid_jwt = _make_jwt({"ClientCode": "50011110", "exp": 2000000000})
        valid_info = inspect_fivepaisa_jwt(valid_jwt, now_ts=1900000000.0)
        self.assertTrue(valid_info["is_jwt"])
        self.assertEqual(valid_info["client_code"], "50011110")
        self.assertFalse(valid_info["is_expired"])
        self.assertTrue(valid_info["exp_label"].endswith("IST"))

        expired_info = inspect_fivepaisa_jwt(valid_jwt, now_ts=2100000000.0)
        self.assertTrue(expired_info["is_expired"])

        plain_info = inspect_fivepaisa_jwt("opaque-token")
        self.assertFalse(plain_info["is_jwt"])
        self.assertFalse(plain_info["is_expired"])
        self.assertEqual(plain_info["client_code"], "")

    def test_sidebar_token_url_paste_clear_jwt_expiry_and_query_params(self):
        import providers

        def _make_jwt(claims: dict) -> str:
            hdr = base64.urlsafe_b64encode(b'{"alg":"HS256"}').decode("ascii").rstrip("=")
            body = base64.urlsafe_b64encode(json.dumps(claims).encode("utf-8")).decode("ascii").rstrip("=")
            return f"{hdr}.{body}.sig"

        at = AppTest.from_file(os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")), "app.py"), default_timeout=15)
        with (
            patch.object(universe, "get_fno_universe", return_value=(["RELIANCE.NS"], "Mock")),
            patch.object(providers, "exchange_upstox_auth_code", return_value=("UP_TOK_NEW", None)) as mock_exch,
        ):
            at.run()
            [c for c in at.segmented_control if c.label == "Data Source"][0].set_value("Upstox v3").run()
            self.assertTrue(any("Access token required" in c.value for c in at.caption))

            [t for t in at.text_input if t.label == "API Key (client_id)"][0].set_value("CID1").run()
            [t for t in at.text_input if t.label == "API Secret"][0].set_value("CSEC1").run()
            [t for t in at.text_input if t.label == "2️⃣ Auth Code or Full Redirect URL"][0].set_value(
                "http://localhost:8501/?code=AUTH_XYZ&state=ok"
            ).run()
            [b for b in at.button if b.label == "Exchange Code for Token"][0].click().run()

            mock_exch.assert_called_once_with("AUTH_XYZ", "CID1", "CSEC1", "http://localhost:8501")
            self.assertEqual(list(at.exception), [])
            self.assertEqual(at.session_state.upstox_access_token, "UP_TOK_NEW")
            self.assertEqual(
                [t for t in at.text_input if t.label == "Upstox Access Token (Daily)"][0].value,
                "UP_TOK_NEW",
            )
            self.assertTrue(any("Upstox token saved" in s.value for s in at.success))

            # Clear button resets token cleanly without stale widget text
            with patch.object(providers, "stop_upstox_stream"):
                [b for b in at.button if b.label == "✕"][0].click().run()
            self.assertEqual(at.session_state.upstox_access_token, "")
            self.assertEqual(
                [t for t in at.text_input if t.label == "Upstox Access Token (Daily)"][0].value,
                "",
            )

            # Switch to 5paisa Xstream: test auto ClientCode extraction & expired JWT warning
            [c for c in at.segmented_control if c.label == "Data Source"][0].set_value("5paisa Xstream").run()
            valid_jwt = _make_jwt({"ClientCode": "55667788", "exp": 4102444800})
            [t for t in at.text_input if t.label == "5paisa Access Token (Daily)"][0].set_value(valid_jwt).run()
            self.assertEqual(
                [t for t in at.text_input if t.label == "5paisa Client Code"][0].value,
                "55667788",
            )
            self.assertTrue(any("JWT loaded · valid until" in c.value for c in at.caption))

            expired_jwt = _make_jwt({"ClientCode": "55667788", "exp": 1700000000})
            [t for t in at.text_input if t.label == "5paisa Access Token (Daily)"][0].set_value(expired_jwt).run()
            self.assertTrue(any("5paisa JWT expired" in w.value for w in at.warning))

        # Verify ?code=... OAuth callback query parameter auto-switches to Upstox v3 & captures code
        at_qp = AppTest.from_file(os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")), "app.py"), default_timeout=15)
        at_qp.query_params["code"] = "AUTO_CODE_77"
        with patch.object(universe, "get_fno_universe", return_value=(["RELIANCE.NS"], "Mock")):
            at_qp.run()
        self.assertEqual(list(at_qp.exception), [])
        self.assertEqual(at_qp.session_state.data_source_choice, "Upstox v3")
        self.assertEqual(
            [t for t in at_qp.text_input if t.label == "2️⃣ Auth Code or Full Redirect URL"][0].value,
            "AUTO_CODE_77",
        )
        self.assertNotIn("code", at_qp.query_params)

    def test_cross_tab_oauth_state_handoff_and_local_token_persistence(self):
        from urllib.parse import parse_qs, urlparse
        import providers
        import ui.auth_helpers as auth_helpers

        # 1. Cross-tab OAuth state handoff WITHOUT st.secrets or local disk store
        at_a = AppTest.from_file(os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")), "app.py"), default_timeout=15)
        with patch.object(universe, "get_fno_universe", return_value=(["RELIANCE.NS"], "Mock")):
            at_a.run()
            [c for c in at_a.segmented_control if c.label == "Data Source"][0].set_value("Upstox v3").run()
            [t for t in at_a.text_input if t.label == "API Key (client_id)"][0].set_value("CID_TAB").run()
            [t for t in at_a.text_input if t.label == "API Secret"][0].set_value("CSEC_TAB").run()

            login_url = providers.build_upstox_login_url(
                "CID_TAB",
                "http://localhost:8501",
                state=at_a.session_state._u_oauth_nonce,
            )
            state_nonce = parse_qs(urlparse(login_url).query)["state"][0]

        at_b = AppTest.from_file(os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")), "app.py"), default_timeout=15)
        at_b.query_params["code"] = "REDIR_CODE_55"
        at_b.query_params["state"] = state_nonce
        with (
            patch.object(universe, "get_fno_universe", return_value=(["RELIANCE.NS"], "Mock")),
            patch.object(providers, "exchange_upstox_auth_code", return_value=("TOK_CROSS_TAB", None)) as mock_exch,
        ):
            at_b.run()

        mock_exch.assert_called_once_with("REDIR_CODE_55", "CID_TAB", "CSEC_TAB", "http://localhost:8501")
        self.assertEqual(at_b.session_state.upstox_access_token, "TOK_CROSS_TAB")
        self.assertIsNone(auth_helpers.pop_oauth_handoff(state_nonce))

        # 2. Local token/key store preserves today's token, expires yesterday's token, and checks localhost host
        self.assertTrue(auth_helpers.is_local_host_request({"Host": "localhost:8501"}))
        self.assertTrue(auth_helpers.is_local_host_request({"Host": "127.0.0.1:8501"}))
        self.assertFalse(auth_helpers.is_local_host_request({"Host": "myscanner.streamlit.app"}))

        with tempfile.TemporaryDirectory() as tmpdir:
            tok_file = Path(tmpdir) / "broker_tokens.json"
            with patch.object(auth_helpers, "LOCAL_TOKENS_FILE", tok_file):
                today_d = date(2026, 11, 28)
                yesterday_d = date(2026, 11, 27)
                auth_helpers.save_local_broker_auth(
                    "upstox",
                    {"access_token": "TOK_TODAY", "client_id": "CID_L", "client_secret": "CSEC_L"},
                    today=today_d,
                )
                loaded_today = auth_helpers.load_local_broker_auth("upstox", today=today_d)
                self.assertEqual(loaded_today["access_token"], "TOK_TODAY")
                self.assertEqual(loaded_today["client_id"], "CID_L")

                # Expired relative to next day -> access_token cleared while API keys stay saved
                loaded_tomorrow = auth_helpers.load_local_broker_auth("upstox", today=date(2026, 11, 29))
                self.assertEqual(loaded_tomorrow["access_token"], "")
                self.assertEqual(loaded_tomorrow["client_id"], "CID_L")

                auth_helpers.save_local_broker_auth("upstox", {}, clear_token=True, today=yesterday_d)
                cleared = auth_helpers.load_local_broker_auth("upstox", today=today_d)
                self.assertEqual(cleared["access_token"], "")
                self.assertEqual(cleared["client_id"], "CID_L")

    def test_detect_default_redirect_uri_and_secrets_override_in_session(self):
        import streamlit as st
        import ui.auth_helpers as auth_helpers

        self.assertEqual(
            auth_helpers.detect_default_redirect_uri("https://fno-vol.streamlit.app/?code=123", {}),
            "https://fno-vol.streamlit.app",
        )
        self.assertEqual(
            auth_helpers.detect_default_redirect_uri("https://fno-vol.streamlit.app/", {}),
            "https://fno-vol.streamlit.app",
        )
        self.assertEqual(
            auth_helpers.detect_default_redirect_uri(None, {"Host": "fno-vol.streamlit.app", "X-Forwarded-Proto": "https"}),
            "https://fno-vol.streamlit.app",
        )
        self.assertEqual(
            auth_helpers.detect_default_redirect_uri(None, {"Host": "localhost:8501"}),
            "http://localhost:8501",
        )
        self.assertEqual(auth_helpers.detect_default_redirect_uri(None, {}), "http://localhost:8501")

        # Hosted app URL auto-populates Redirect URI and updated st.secrets overrides stale session default
        at = AppTest.from_file(os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")), "app.py"), default_timeout=15)
        with (
            patch.object(universe, "get_fno_universe", return_value=(["RELIANCE.NS"], "Mock")),
            patch.object(type(st.context), "url", new="https://hosted-scanner.streamlit.app/"),
        ):
            at.run()
            [c for c in at.segmented_control if c.label == "Data Source"][0].set_value("Upstox v3").run()
            self.assertEqual(
                [t for t in at.text_input if t.label == "Redirect URI"][0].value,
                "https://hosted-scanner.streamlit.app",
            )
            at.secrets["upstox"] = {
                "client_id": "CID_CLOUD",
                "client_secret": "CSEC_CLOUD",
                "redirect_uri": "https://custom-cloud.streamlit.app",
            }
            at.run()
            self.assertEqual(at.session_state._u_redirect_uri, "https://custom-cloud.streamlit.app")
            self.assertTrue(
                any("https://custom-cloud.streamlit.app" in c.value for c in at.caption)
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
