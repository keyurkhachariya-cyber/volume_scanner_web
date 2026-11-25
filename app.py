"""
Relative Volume Scanner (F&O Stocks) - Streamlit Web App
Today's cumulative volume till now vs Previous day's volume till same time.
"""

from datetime import datetime, time
import streamlit as st
import pandas as pd

from universe import get_fno_universe
from scanner import prune_cache, run_scan
from holidays import get_holidays, is_market_open, get_previous_trading_day, IST

MARKET_CLOSE_TIME = time(15, 30)

st.set_page_config(
    page_title="F&O Relative Volume Scanner",
    page_icon="📈",
    layout="wide",
)

st.title("📈 F&O Relative Volume Scanner")
st.caption("Today's volume till now vs Previous trading day volume till same clock time (IST)")

DEFAULT_COLUMNS = [
    "Symbol",
    "LTP",
    "Vol_Today",
    "Vol_Prev_Same_Time",
    "Ratio",
    "Pct_Change",
    "Last_Bar_Time",
]

ALL_COLUMNS = [
    "Symbol",
    "LTP",
    "Vol_Today",
    "Vol_Prev_Same_Time",
    "Ratio",
    "Pct_Change",
    "Last_Bar_Time",
    "Gap_Pct",
    "Prev_Close",
    "Day_Open",
    "Day_High",
    "Day_Low",
    "Range_Pct",
    "Vol_Diff",
    "Prev_Full_Vol",
    "Turnover_Cr",
]

COLUMN_LABELS = {
    "Symbol": "Symbol",
    "LTP": "LTP",
    "Vol_Today": "Vol Today (till now)",
    "Vol_Prev_Same_Time": "Vol Prev (same time)",
    "Ratio": "Ratio",
    "Pct_Change": "% Chg",
    "Last_Bar_Time": "Last Bar (IST)",
    "Gap_Pct": "Gap %",
    "Prev_Close": "Prev Close",
    "Day_Open": "Day Open",
    "Day_High": "Day High",
    "Day_Low": "Day Low",
    "Range_Pct": "Day Range %",
    "Vol_Diff": "Vol Diff",
    "Prev_Full_Vol": "Prev Full Day Vol",
    "Turnover_Cr": "Turnover (₹ Cr)",
}

COLUMN_CONFIG = {
    "Symbol": st.column_config.TextColumn("Symbol", width="medium"),
    "LTP": st.column_config.NumberColumn("LTP", format="%.2f"),
    "Vol_Today": st.column_config.NumberColumn("Vol Today (till now)", format="localized"),
    "Vol_Prev_Same_Time": st.column_config.NumberColumn("Vol Prev (same time)", format="localized"),
    "Ratio": st.column_config.NumberColumn("Ratio", format="%.2f×"),
    "Pct_Change": st.column_config.NumberColumn("% Chg", format="%+.2f%%"),
    "Last_Bar_Time": st.column_config.TextColumn(
        "Last Bar (IST)",
        help="Latest regular-hours bar timestamp (IST) used for bar-aligned baseline lookup.",
    ),
    "Gap_Pct": st.column_config.NumberColumn("Gap %", format="%+.2f%%"),
    "Prev_Close": st.column_config.NumberColumn("Prev Close", format="%.2f"),
    "Day_Open": st.column_config.NumberColumn("Day Open", format="%.2f"),
    "Day_High": st.column_config.NumberColumn("Day High", format="%.2f"),
    "Day_Low": st.column_config.NumberColumn("Day Low", format="%.2f"),
    "Range_Pct": st.column_config.NumberColumn("Day Range %", format="%.2f%%"),
    "Vol_Diff": st.column_config.NumberColumn("Vol Diff", format="localized"),
    "Prev_Full_Vol": st.column_config.NumberColumn("Prev Full Day Vol", format="localized"),
    "Turnover_Cr": st.column_config.NumberColumn("Turnover (₹ Cr)", format="%.2f"),
}


@st.cache_resource
def _run_startup_housekeeping() -> bool:
    """Prune stale/legacy cache files once per Streamlit server lifecycle."""
    prune_cache()
    return True


_run_startup_housekeeping()

if "universe_symbols" not in st.session_state:
    syms, src = get_fno_universe(force_refresh=False)
    st.session_state.universe_symbols = syms
    st.session_state.universe_source = src

# Session state initialization (keyed per bar interval so switching 1m/5m preserves snapshots)
if "results_by_interval" not in st.session_state:
    st.session_state.results_by_interval = {}
if "scan_times_by_interval" not in st.session_state:
    st.session_state.scan_times_by_interval = {}
if "selected_columns" not in st.session_state:
    st.session_state.selected_columns = DEFAULT_COLUMNS.copy()
if "_skip_auto_scan_once" not in st.session_state:
    st.session_state._skip_auto_scan_once = False
if "_last_filter_state" not in st.session_state:
    st.session_state._last_filter_state = None
if "_post_close_attempts_by_interval" not in st.session_state:
    st.session_state._post_close_attempts_by_interval = {}
if "_last_scan_empty_by_interval" not in st.session_state:
    st.session_state._last_scan_empty_by_interval = {}
if "_cached_prev_day" not in st.session_state:
    st.session_state._cached_prev_day = None
if "_last_interval" not in st.session_state:
    st.session_state._last_interval = None
if "_scan_started_by_interval" not in st.session_state:
    st.session_state._scan_started_by_interval = {}

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
    exclude_gap_up = st.checkbox(
        "Exclude Gap-Up stocks",
        value=False,
        help="Hide stocks that opened above previous close by more than the gap threshold.",
    )
    exclude_gap_down = st.checkbox(
        "Exclude Gap-Down stocks",
        value=False,
        help="Hide stocks that opened below previous close by more than the gap threshold.",
    )
    gap_threshold = 0.5
    if exclude_gap_up or exclude_gap_down:
        gap_threshold = st.number_input(
            "Gap threshold (%)",
            min_value=0.0,
            max_value=20.0,
            value=0.5,
            step=0.1,
            help="Gap % = (Day Open - Prev Close) / Prev Close * 100. Set 0.0 to exclude any gap.",
        )
    interval = st.selectbox(
        "Bar Interval",
        options=["1m", "5m"],
        index=0,
        help=(
            "1-minute is more precise for live comparisons. 5-minute has fewer Yahoo gaps, "
            "though a forming 5m bar compares against the full previous-day 5m bar until that 5-minute window completes."
        ),
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
            help="Interval between automated background scans while NSE is open.",
        )
    st.markdown("---")
    if st.button("🔄 Sync NSE F&O List", width="stretch"):
        with st.spinner("Fetching latest F&O list and trading calendar from NSE..."):
            syms, src = get_fno_universe(force_refresh=True)
            get_holidays(force_refresh=True)
            st.session_state.universe_symbols = syms
            st.session_state.universe_source = src
            st.session_state._cached_prev_day = None
            st.session_state._skip_auto_scan_once = True

    active_universe = st.session_state.universe_symbols
    st.info(
        f"Universe size: **{len(active_universe)}** F&O stocks\n\n"
        f"Source: {st.session_state.universe_source}"
    )

# Detect sidebar filter/config tweaks so changing filters never hijacks into a network scan
current_filter_state = (
    float(min_ratio),
    int(min_volume),
    bool(exclude_gap_up),
    bool(exclude_gap_down),
    float(gap_threshold),
    int(max_workers),
)
if (
    st.session_state._last_filter_state is not None
    and st.session_state._last_filter_state != current_filter_state
):
    st.session_state._skip_auto_scan_once = True
elif (
    st.session_state._last_interval is not None
    and st.session_state._last_interval != interval
    and interval in st.session_state.scan_times_by_interval
):
    st.session_state._skip_auto_scan_once = True
st.session_state._last_filter_state = current_filter_state
st.session_state._last_interval = interval


def _mark_ui_interaction() -> None:
    st.session_state._skip_auto_scan_once = True


def _reset_default_columns() -> None:
    st.session_state.selected_columns = DEFAULT_COLUMNS.copy()
    st.session_state._skip_auto_scan_once = True


def _should_auto_scan(
    now: datetime,
    last_scan_dt: datetime | None,
    refresh_seconds: int,
    market_open: bool,
    holidays_set: set,
    skip_auto_scan: bool,
    post_close_attempts: int = 0,
    max_post_close_attempts: int = 2,
    last_scan_started_dt: datetime | None = None,
) -> bool:
    """
    Decide whether a scheduled auto-refresh scan should execute:
    - Never run during pure UI/filter interactions.
    - Poll every refresh_seconds while NSE is open.
    - Allow a bounded post-close snapshot (up to max_post_close_attempts on empty data) after 15:30 IST.
    - Stay idle pre-market, on weekends, and on holidays.
    """
    if skip_auto_scan:
        return False

    if market_open:
        cadence_ref = last_scan_started_dt or last_scan_dt
        if cadence_ref is None or cadence_ref.date() != now.date():
            return True
        elapsed = (now - cadence_ref).total_seconds()
        return elapsed >= max(refresh_seconds - 1, 1)

    is_trading_day = now.weekday() < 5 and now.date() not in holidays_set
    if is_trading_day and now.time() > MARKET_CLOSE_TIME:
        if post_close_attempts >= max_post_close_attempts:
            return False
        market_close_dt = now.replace(hour=15, minute=30, second=0, microsecond=0)
        if last_scan_dt is None or last_scan_dt.date() < now.date() or last_scan_dt < market_close_dt:
            return True

    return False


def render_scanner(
    symbols: list[str],
    min_ratio: float,
    min_volume: int,
    exclude_gap_up: bool,
    exclude_gap_down: bool,
    gap_threshold: float,
    interval: str,
    max_workers: int,
    auto_refresh: bool,
    refresh_seconds: int,
):
    now = datetime.now(IST)
    holidays_set = get_holidays()
    market_open = is_market_open(now)

    last_scan_dt = st.session_state.scan_times_by_interval.get(interval)
    last_scan_started_dt = st.session_state._scan_started_by_interval.get(interval)
    skip_once = bool(st.session_state._skip_auto_scan_once)
    st.session_state._skip_auto_scan_once = False

    attempt_rec = st.session_state._post_close_attempts_by_interval.get(interval)
    post_close_attempts = attempt_rec[1] if (attempt_rec and attempt_rec[0] == now.date()) else 0

    should_scan = False
    if auto_refresh:
        # Checked before widgets below; manual button click also forces a scan once rendered.
        should_scan = _should_auto_scan(
            now=now,
            last_scan_dt=last_scan_dt,
            refresh_seconds=refresh_seconds,
            market_open=market_open,
            holidays_set=holidays_set,
            skip_auto_scan=skip_once,
            post_close_attempts=post_close_attempts,
            last_scan_started_dt=last_scan_started_dt,
        )

    col1, col2, col3 = st.columns(3)
    col1.metric("Current IST", now.strftime("%Y-%m-%d %H:%M:%S"))
    prev_day_slot = col2.empty()
    col3.metric("Market Status", "🟢 OPEN" if market_open else "🔴 CLOSED")

    st.markdown("---")

    col_btn, col_info = st.columns([1, 2])
    with col_btn:
        scan_clicked = st.button("🔄 Scan Now", type="primary", width="stretch")
    if scan_clicked:
        should_scan = True

    cached_prev = st.session_state._cached_prev_day
    if should_scan or cached_prev is None or cached_prev[0] != now.date():
        prev_day = get_previous_trading_day(now.date())
        st.session_state._cached_prev_day = (now.date(), prev_day)
    else:
        prev_day = cached_prev[1]
    prev_day_slot.metric("Previous Trading Day", prev_day.strftime("%Y-%m-%d"))

    with col_info:
        info_slot = st.empty()
        if auto_refresh and should_scan and not market_open and not scan_clicked:
            info_slot.caption("⏱️ **Auto-refresh**: running final post-close snapshot before pausing.")

    # Reserve layout slots before slow network calls to minimize stale greying & remounting
    status_slot = st.container()
    results_slot = st.container()

    if should_scan:
        with status_slot:
            with st.spinner(f"Scanning {len(symbols)} F&O stocks ({interval} bars)..."):
                scan_started_dt = datetime.now(IST)
                # Fetch unfiltered snapshot so sidebar filter changes apply instantly in memory
                raw_df = run_scan(
                    symbols=symbols,
                    min_ratio=0.0,
                    min_volume=0,
                    interval=interval,
                    max_workers=max_workers,
                )
                if not raw_df.empty:
                    st.session_state.results_by_interval[interval] = raw_df
                    last_scan_dt = datetime.now(IST)
                    st.session_state.scan_times_by_interval[interval] = last_scan_dt
                    st.session_state._scan_started_by_interval[interval] = scan_started_dt
                    st.session_state._last_scan_empty_by_interval.pop(interval, None)
                    if not market_open:
                        st.session_state._post_close_attempts_by_interval.pop(interval, None)
                else:
                    st.session_state._last_scan_empty_by_interval[interval] = datetime.now(IST)
                    if not scan_clicked and not market_open and now.time() > MARKET_CLOSE_TIME:
                        st.session_state._post_close_attempts_by_interval[interval] = (
                            now.date(),
                            post_close_attempts + 1,
                        )

    if auto_refresh:
        now_after = datetime.now(IST)
        if is_market_open(now_after):
            info_slot.caption(
                f"⏱️ **Auto-refresh active** (every {refresh_seconds}s). "
                "Updating in the background without full-page reloads."
            )
        else:
            info_slot.caption("⏸️ **Auto-refresh paused** (market closed). Manual **Scan Now** still works anytime.")

    # Client-side filtering on the cached scan snapshot for the active interval
    raw_df = st.session_state.results_by_interval.get(interval, pd.DataFrame())
    raw_empty_dt = st.session_state._last_scan_empty_by_interval.get(interval)
    empty_scan_dt = raw_empty_dt if (raw_empty_dt is not None and raw_empty_dt.date() == now.date()) else None
    gap_missing = False
    gap_excluded_count = 0

    if raw_df.empty:
        df = raw_df
    else:
        working_df = raw_df
        if "Gap_Pct" not in working_df.columns and {"Day_Open", "Prev_Close"}.issubset(working_df.columns):
            working_df = working_df.copy()
            valid_prev = working_df["Prev_Close"].notna() & (working_df["Prev_Close"] > 0)
            working_df["Gap_Pct"] = float("nan")
            working_df.loc[valid_prev, "Gap_Pct"] = (
                (working_df.loc[valid_prev, "Day_Open"] - working_df.loc[valid_prev, "Prev_Close"])
                / working_df.loc[valid_prev, "Prev_Close"]
            ) * 100

        base_mask = (working_df["Ratio"] >= min_ratio) & (working_df["Vol_Today"] >= int(min_volume))
        mask = base_mask.copy()

        if exclude_gap_up or exclude_gap_down:
            if "Gap_Pct" not in working_df.columns:
                gap_missing = True
            else:
                gap_vals = pd.to_numeric(working_df["Gap_Pct"], errors="coerce")
                if exclude_gap_up:
                    mask &= gap_vals.isna() | (gap_vals <= gap_threshold)
                if exclude_gap_down:
                    mask &= gap_vals.isna() | (gap_vals >= -gap_threshold)
                gap_excluded_count = int(base_mask.sum() - mask.sum())

        df = working_df[mask].reset_index(drop=True)

    with status_slot:
        if last_scan_dt is not None:
            gap_note = ""
            if (exclude_gap_up or exclude_gap_down) and not gap_missing:
                gap_note = f" | Excluded by gap filter: **{gap_excluded_count}**"
            st.success(
                f"Last scan: {last_scan_dt.strftime('%H:%M:%S IST')} "
                f"({interval} bars) | "
                f"Showing **{len(df)}** of **{len(raw_df)}** scanned stocks{gap_note}"
            )
            if empty_scan_dt is not None and not raw_df.empty:
                st.warning(
                    f"Latest scan at {empty_scan_dt.strftime('%H:%M:%S IST')} returned no data — "
                    "showing the previous snapshot."
                )
            if gap_missing:
                st.warning(
                    "Current results were scanned before Gap % data was added — click **Scan Now** once to apply the gap filter."
                )
        elif empty_scan_dt is not None:
            st.warning(
                f"Scan at {empty_scan_dt.strftime('%H:%M:%S IST')} returned no intraday data for today "
                "(market may be pre-open, closed, or Yahoo Finance has no bars yet)."
            )
        elif st.session_state.results_by_interval:
            st.info(f"No **{interval}** snapshot yet — click **Scan Now** to fetch {interval} bars.")

    with results_slot:
        if df.empty:
            if last_scan_dt is None and empty_scan_dt is None:
                st.info("Click **Scan Now** or enable **Auto-refresh** in the sidebar to run the scan.")
            elif not raw_df.empty:
                st.warning("No stocks matched the current filters. Try lowering the Minimum Ratio or Volume.")
        else:
            with st.popover("⚙️ Customize Columns"):
                st.multiselect(
                    "Visible columns (add, remove, or reorder)",
                    options=ALL_COLUMNS,
                    format_func=lambda col: COLUMN_LABELS.get(col, col),
                    key="selected_columns",
                    on_change=_mark_ui_interaction,
                )
                st.button("Reset to Default", on_click=_reset_default_columns)

            visible_cols = [c for c in st.session_state.selected_columns if c in df.columns]
            if not visible_cols:
                st.warning("Select at least one column in **Customize Columns** to display results.")
            else:
                display_df = df[visible_cols]
                active_config = {c: COLUMN_CONFIG[c] for c in visible_cols if c in COLUMN_CONFIG}

                st.dataframe(
                    display_df,
                    width="stretch",
                    hide_index=True,
                    column_config=active_config,
                    key="scanner_table",
                )

                csv = display_df.to_csv(index=False).encode("utf-8")
                csv_ts = (last_scan_dt or now).strftime("%Y%m%d_%H%M")
                st.download_button(
                    "⬇️ Download CSV",
                    data=csv,
                    file_name=f"relative_volume_{interval}_{csv_ts}.csv",
                    mime="text/csv",
                    key=f"download_csv_{interval}",
                    on_click="ignore",
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
    exclude_gap_up=exclude_gap_up,
    exclude_gap_down=exclude_gap_down,
    gap_threshold=gap_threshold,
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
