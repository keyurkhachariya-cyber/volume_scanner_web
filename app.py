"""
Relative Volume Scanner (F&O Stocks) - Streamlit Web App
Today's cumulative volume till now vs Previous day's volume till same time.
"""

from datetime import datetime
import streamlit as st
import pandas as pd

from universe import get_fno_universe
from scanner import run_scan
from holidays import is_market_open, get_previous_trading_day, IST

st.set_page_config(
    page_title="F&O Relative Volume Scanner",
    page_icon="📈",
    layout="wide",
)

st.title("📈 F&O Relative Volume Scanner")
st.caption("Today's volume till now vs Previous trading day volume till same clock time (IST)")

if "universe_symbols" not in st.session_state:
    syms, src = get_fno_universe(force_refresh=False)
    st.session_state.universe_symbols = syms
    st.session_state.universe_source = src

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
    max_workers = st.slider("Parallel download threads", 4, 16, 8)

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
    if st.button("🔄 Sync NSE F&O List", use_container_width=True):
        with st.spinner("Fetching latest F&O list from NSE..."):
            syms, src = get_fno_universe(force_refresh=True)
            st.session_state.universe_symbols = syms
            st.session_state.universe_source = src

    active_universe = st.session_state.universe_symbols
    st.info(
        f"Universe size: **{len(active_universe)}** F&O stocks\n\n"
        f"Source: {st.session_state.universe_source}"
    )

# Session state initialization
if "raw_results_df" not in st.session_state:
    st.session_state.raw_results_df = pd.DataFrame()
if "last_scan_time" not in st.session_state:
    st.session_state.last_scan_time = None
if "last_scan_interval" not in st.session_state:
    st.session_state.last_scan_interval = None


def render_scanner(
    symbols: list[str],
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
        with st.spinner(f"Scanning {len(symbols)} F&O stocks..."):
            # Fetch unfiltered snapshot so sidebar filter changes apply instantly in memory
            raw_df = run_scan(
                symbols=symbols,
                min_ratio=0.0,
                min_volume=0,
                interval=interval,
                max_workers=max_workers,
            )
            st.session_state.raw_results_df = raw_df
            st.session_state.last_scan_time = datetime.now(IST)
            st.session_state.last_scan_interval = interval

    # Client-side filtering on the cached scan snapshot
    raw_df = st.session_state.raw_results_df
    if raw_df.empty:
        df = raw_df
    else:
        df = raw_df[
            (raw_df["Ratio"] >= min_ratio) & (raw_df["Vol_Today"] >= int(min_volume))
        ].reset_index(drop=True)

    if st.session_state.last_scan_time:
        st.success(
            f"Last scan: {st.session_state.last_scan_time.strftime('%H:%M:%S IST')} "
            f"({st.session_state.last_scan_interval} bars) | "
            f"Showing **{len(df)}** of **{len(raw_df)}** scanned stocks"
        )
        if st.session_state.last_scan_interval and st.session_state.last_scan_interval != interval:
            st.info(f"Bar interval changed to **{interval}** — click **Scan Now** to fetch {interval} bars.")

    if df.empty:
        if st.session_state.last_scan_time is None:
            st.info("Click **Scan Now** or enable **Auto-refresh** in the sidebar to run the scan.")
        else:
            st.warning("No stocks matched the current filters. Try lowering the Minimum Ratio or Volume.")
    else:
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Symbol": st.column_config.TextColumn("Symbol", width="medium"),
                "LTP": st.column_config.NumberColumn("LTP", format="%.2f"),
                "Vol_Today": st.column_config.NumberColumn("Vol Today (till now)", format="localized"),
                "Vol_Prev_Same_Time": st.column_config.NumberColumn("Vol Prev (same time)", format="localized"),
                "Ratio": st.column_config.NumberColumn("Ratio", format="%.2f×"),
                "Pct_Change": st.column_config.NumberColumn("% Chg", format="%+.2f%%"),
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
    symbols=active_universe,
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
