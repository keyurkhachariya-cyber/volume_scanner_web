"""
Single-source table column schemas, labels, sort options, and user guide for the screener UI.
"""

from typing import Any, Dict, Tuple
import streamlit as st

# Each entry: (column_name, display_label, default_visible, column_kind, column_config_kwargs)
_COLUMN_SPECS: Tuple[Tuple[str, str, bool, str, Dict[str, Any]], ...] = (
    (
        "Signal",
        "Signal",
        True,
        "text",
        {
            "pinned": True,
            "help": "🟢 Bullish: Ratio > 1.0×, OI Chg > +10%, % Chg > +1.5% | 🔴 Bearish: Ratio > 1.0×, OI Chg > +10%, % Chg < -1.5%",
        },
    ),
    ("Symbol", "Symbol", True, "text", {"width": "medium", "pinned": True}),
    ("LTP", "LTP", True, "number", {"format": "%.2f"}),
    (
        "Ratio",
        "Ratio",
        True,
        "number",
        {
            "format": "%.2f×",
            "help": "Today's cumulative volume till now divided by previous trading day's volume till the same IST clock time.",
        },
    ),
    (
        "Vol_Burst_15m",
        "15m Vol Burst",
        True,
        "number",
        {
            "format": "%.2f×",
            "help": "Last 15m volume-per-minute pace vs today's average pace (>1.5× active right now; <0.5× faded morning spike).",
        },
    ),
    (
        "Pct_Change",
        "% Chg",
        True,
        "number",
        {
            "format": "%+.2f%%",
            "help": "Price % change from Previous Close (includes opening gap; compare with From Open %).",
        },
    ),
    (
        "From_Open_Pct",
        "From Open %",
        True,
        "number",
        {
            "format": "%+.2f%%",
            "help": "Intraday move since 09:15 Open ((LTP - Day Open) / Day Open). Positive = green intraday candle.",
        },
    ),
    (
        "VWAP_Dist_Pct",
        "VWAP Dist %",
        True,
        "number",
        {
            "format": "%+.2f%%",
            "help": "Distance of LTP from today's VWAP (+ve = trading above VWAP, -ve = below VWAP).",
        },
    ),
    (
        "Range_Pos_Pct",
        "Range Pos %",
        True,
        "number",
        {
            "format": "%.0f%%",
            "help": "Position of LTP within Day High–Low range: 100% = Day High, 0% = Day Low.",
        },
    ),
    (
        "OI_Chg_Pct",
        "OI Chg % (Day)",
        True,
        "number",
        {"format": "%+.2f%%", "help": "Today's % change in Open Interest vs previous trading day close."},
    ),
    (
        "OI_Buildup",
        "OI Buildup",
        True,
        "text",
        {"help": "Price Δ vs Day OI Δ classification: Long Buildup, Short Buildup, Short Covering, Long Unwinding."},
    ),
    ("Vol_Today", "Vol Today (till now)", False, "number", {"format": "localized"}),
    ("Vol_Prev_Same_Time", "Vol Prev (same time)", False, "number", {"format": "localized"}),
    (
        "OI",
        "OI (Lots)",
        False,
        "number",
        {"format": "localized", "help": "Latest aggregate F&O Open Interest from NSE."},
    ),
    (
        "OI_Chg",
        "OI Chg (Day)",
        False,
        "number",
        {"format": "localized", "help": "Today's net OI change in lots (OI - Prev OI)."},
    ),
    (
        "Last_Bar_Time",
        "Last Bar (IST)",
        False,
        "text",
        {"help": "Latest regular-hours bar timestamp (IST) used for bar-aligned baseline lookup."},
    ),
    (
        "Gap_Pct",
        "Gap %",
        False,
        "number",
        {"format": "%+.2f%%", "help": "Opening gap % = (Day Open - Prev Close) / Prev Close × 100."},
    ),
    ("Range_Pct", "Day Range %", False, "number", {"format": "%.2f%%"}),
    ("Turnover_Cr", "Turnover (₹ Cr)", False, "number", {"format": "%.2f"}),
)

ALL_COLUMNS = [col for col, *_ in _COLUMN_SPECS]
DEFAULT_COLUMNS = [col for col, _, is_default, *_ in _COLUMN_SPECS if is_default]
COLUMN_LABELS = {col: label for col, label, *_ in _COLUMN_SPECS}


def build_column_config() -> Dict[str, Any]:
    """Build Streamlit column_config mapping from _COLUMN_SPECS."""
    builders = {
        "text": st.column_config.TextColumn,
        "number": st.column_config.NumberColumn,
    }
    return {
        col: builders[kind](label, **kwargs)
        for col, label, _, kind, kwargs in _COLUMN_SPECS
    }


COLUMN_CONFIG = build_column_config()

SORT_OPTIONS = {
    "Ratio (High → Low)": ("Ratio", False, False),
    "Signal (Bullish / Bearish first)": ("Signal", False, False),
    "15m Vol Burst (High → Low)": ("Vol_Burst_15m", False, False),
    "VWAP Dist % (High → Low)": ("VWAP_Dist_Pct", False, False),
    "Range Pos % (Near Day High)": ("Range_Pos_Pct", False, False),
    "From Open % (High → Low)": ("From_Open_Pct", False, False),
    "OI Chg % Day (High → Low)": ("OI_Chg_Pct", False, False),
    "OI Chg % Day (Low → High)": ("OI_Chg_Pct", True, False),
    "|OI Chg %| Day (Largest Moves)": ("OI_Chg_Pct", False, True),
    "OI Chg Lots Day (High → Low)": ("OI_Chg", False, False),
    "Price % Chg (High → Low)": ("Pct_Change", False, False),
    "Turnover ₹ Cr (High → Low)": ("Turnover_Cr", False, False),
}

COLUMN_GUIDE_MARKDOWN = """
| Column | What It Measures | How to Read for Today's Momentum |
| :--- | :--- | :--- |
| **Signal** | High-conviction combo highlight | **🟢 Bullish:** `Ratio > 1.0×`, `OI Chg > +10%`, `% Chg > +1.5%`<br>**🔴 Bearish:** `Ratio > 1.0×`, `OI Chg > +10%`, `% Chg < -1.5%` |
| **Ratio** | Today's vol till now ÷ Prev day vol till same clock time | **`≥ 1.5×`:** strong participation vs yesterday; **`> 2.5×`:** unusual institutional activity |
| **15m Vol Burst** | Last 15m vol/min ÷ today's avg vol/min | **`> 1.5×`:** accelerating *right now*; **`< 0.5×`:** stale morning spike that cooled off |
| **% Chg** | Price move vs Previous Close | Overall daily gain/loss (includes opening gap) |
| **From Open %** | `(LTP - Day Open) / Day Open` | **Green vs Red intraday candle:** Avoid chasing `+3%` stocks whose `From Open %` is negative (faded gap-ups) |
| **VWAP Dist %** | `(LTP - VWAP) / VWAP` | **Bullish:** `> 0%` (ideally `+0.2%` to `+2%`, not overly stretched); **Bearish:** `< 0%` |
| **Range Pos %** | Where LTP sits between Day Low (`0%`) & Day High (`100%`) | **Bullish:** `70–100%` (holding near highs); **Bearish:** `0–30%` (holding near lows) |
| **OI Chg (Day) / %** | Live NSE F&O Open Interest change vs yesterday close | **`> +10%`:** strong fresh buildup confirming real positioning |
| **OI Buildup** | Price Δ vs Day OI Δ | **Long Buildup** (`+P, +OI`), **Short Buildup** (`-P, +OI`), **Short Covering** (`+P, -OI`), **Long Unwinding** (`-P, -OI`) |

**Quick Playbook:**
1. Use the **🟢 Bullish / 🔴 Bearish** pills above the table (or sort by **Signal** / **15m Vol Burst**) to spot active setups.
2. Enable **Momentum Quality Filters** in the sidebar to auto-hide stocks trading on the wrong side of **VWAP**, red-candle gap-ups (**From Open %**), weak closes (**Range Pos %**), or dead morning spikes (**15m Vol Burst**).
"""
