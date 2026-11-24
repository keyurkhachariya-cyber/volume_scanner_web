"""
Relative Volume Scanner (F&O Stocks) - Streamlit Web App
Today's cumulative volume till now vs Previous day's volume till same time.
"""

from datetime import datetime
import streamlit as st
import pandas as pd

from universe import FNO_STOCKS
from scanner import run_scan
from holidays import is_market_open, get_previous_trading_day, IST

st.set_page_config(
    page_title="F&O Relative Volume Scanner",
    page_icon="📈",
    layout="wide",
)

st.title("📈 F&O Relative Volume Scanner")
st.caption("Today's volume till now vs Previous trading day volume till same clock time (IST)")

# Sidebar controls
with st.sidebar:
    st.header("Settings")
    min_ratio = st.number_input(
        "Minimum Ratio (Today / Prev)",
        min_value=0.5,
        max_value=20.0,
        value=1.5,
        step=0.1,
        help="Only show stocks where today's volume till now is at least this multiple of previous day's volume till the same time.",
    )
    min_volume = st.number_input(
        "Minimum Volume Today",
        min_value=0,
        value=50000,
        step=10000,
        help="Filter out low-volume names.",
    )
    interval = st.selectbox(
        "Bar Interval",
        options=["1m", "5m"],
        index=0,
        help="1-minute is more precise but slower and more prone to gaps. 5-minute is more reliable.",
    )
    max_workers = st.slider("Parallel workers", 4, 12, 8)

    st.markdown("---")
    auto_refresh = st.checkbox("Auto-refresh", value=False)
    refresh_seconds = 60
    if auto_refresh:
        refresh_seconds = st.selectbox(
            "Auto-refresh interval",
            options=[30, 60, 120, 300],
            index=1,
            format_func=lambda s: f"{s} seconds" if s < 60 else f"{s // 60} minute{'s' if s > 60 else ''}",
            help="Interval between automated background scans.",
        )
    st.markdown("---")
    st.info(f"Universe size: **{len(FNO_STOCKS)}** F&O stocks")

# Session state initialization
if "results_df" not in st.session_state:
    st.session_state.results_df = pd.DataFrame()
if "last_scan_time" not in st.session_state:
    st.session_state.last_scan_time = None


def render_scanner(
    min_ratio: float,
    min_volume: int,
    interval: str,
    max_workers: int,
    auto_refresh: bool,
    refresh_seconds: int,
):
    now = datetime.now(IST)
    prev_day = get_previous_trading_day(now.date())
    market_open = is_market_open(now)

    col1, col2, col3 = st.columns(3)
    col1.metric("Current IST", now.strftime("%Y-%m-%d %H:%M:%S"))
    col2.metric("Previous Trading Day", prev_day.strftime("%Y-%m-%d"))
    col3.metric("Market Status", "🟢 OPEN" if market_open else "🔴 CLOSED")

    st.markdown("---")

    col_btn, col_info = st.columns([1, 2])
    with col_btn:
        scan_clicked = st.button("🔄 Scan Now", type="primary", use_container_width=True)
    with col_info:
        if auto_refresh:
            st.caption(
                f"⏱️ **Auto-refresh active** (every {refresh_seconds}s). "
                "Updating in the background without full-page reloads."
            )

    should_scan = False
    if scan_clicked:
        should_scan = True
    elif auto_refresh:
        if st.session_state.last_scan_time is None:
            should_scan = True
        else:
            elapsed = (datetime.now(IST) - st.session_state.last_scan_time).total_seconds()
            if elapsed >= max(refresh_seconds - 5, 10):
                should_scan = True

    if should_scan:
        with st.spinner(f"Scanning {len(FNO_STOCKS)} F&O stocks... (this may take 30-90 seconds)"):
            df = run_scan(
                symbols=FNO_STOCKS,
                min_ratio=min_ratio,
                min_volume=int(min_volume),
                interval=interval,
                max_workers=max_workers,
            )
            st.session_state.results_df = df
            st.session_state.last_scan_time = datetime.now(IST)

    # Display results
    df = st.session_state.results_df

    if st.session_state.last_scan_time:
        st.success(
            f"Last scan: {st.session_state.last_scan_time.strftime('%H:%M:%S IST')} | "
            f"Found **{len(df)}** stocks matching filters"
        )

    if df.empty:
        if st.session_state.last_scan_time is None:
            st.info("Click **Scan Now** or enable **Auto-refresh** in the sidebar to run the scan.")
        else:
            st.warning("No stocks matched the current filters. Try lowering the Minimum Ratio or Volume.")
    else:
        display_df = df.copy()
        display_df["Vol_Today"] = display_df["Vol_Today"].apply(lambda x: f"{x:,}")
        display_df["Vol_Prev_Same_Time"] = display_df["Vol_Prev_Same_Time"].apply(lambda x: f"{x:,}")
        display_df["Ratio"] = display_df["Ratio"].apply(lambda x: f"{x:.2f}×")
        if "Pct_Change" in display_df.columns:
            display_df["Pct_Change"] = display_df["Pct_Change"].apply(
                lambda x: f"{x:+.2f}%" if pd.notna(x) else "—"
            )

        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Symbol": st.column_config.TextColumn("Symbol", width="medium"),
                "LTP": st.column_config.NumberColumn("LTP", format="%.2f"),
                "Vol_Today": st.column_config.TextColumn("Vol Today (till now)"),
                "Vol_Prev_Same_Time": st.column_config.TextColumn("Vol Prev (same time)"),
                "Ratio": st.column_config.TextColumn("Ratio"),
                "Pct_Change": st.column_config.TextColumn("% Chg"),
            },
        )

        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇️ Download CSV",
            data=csv,
            file_name=f"relative_volume_{now.strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv",
        )


# Bind fragment dynamically with run_every when auto_refresh is enabled
scanner_fragment = st.fragment(
    render_scanner,
    run_every=refresh_seconds if auto_refresh else None,
)

scanner_fragment(
    min_ratio=min_ratio,
    min_volume=int(min_volume),
    interval=interval,
    max_workers=max_workers,
    auto_refresh=auto_refresh,
    refresh_seconds=refresh_seconds,
)

st.markdown("---")
st.caption(
    "Data source: yfinance (Yahoo Finance). 1-minute data is limited and may have gaps. "
    "This is for educational / research purposes only. Not investment advice."
)
