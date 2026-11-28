"""
Relative Volume Scanner (F&O Stocks) - Streamlit Web App
Today's cumulative volume till now vs Previous day's volume till same time.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import os
import streamlit as st
import pandas as pd

from universe import get_fno_universe
from scanner import prune_cache, run_scan
from holidays import get_holidays, is_market_open, get_previous_trading_day, IST
from oi import OI_BUILDUP_OPTIONS, SIGNAL_BEARISH, SIGNAL_BULLISH
from providers import (
    FivePaisaProvider,
    UpstoxProvider,
    YahooProvider,
    build_fivepaisa_login_url,
    build_upstox_login_url,
    exchange_fivepaisa_request_token,
    exchange_upstox_auth_code,
    login_fivepaisa_totp,
)
from providers.fivepaisa_stream import stop_fivepaisa_stream
from providers.upstox_stream import stop_upstox_stream
from screener_filters import (
    MARKET_CLOSE_TIME,
    filter_and_sort_results,
    should_auto_scan as _should_auto_scan,
    style_scanner_df as _style_scanner_df,
)
from table_config import (
    ALL_COLUMNS,
    COLUMN_CONFIG,
    COLUMN_GUIDE_MARKDOWN,
    COLUMN_LABELS,
    DEFAULT_COLUMNS,
    SORT_OPTIONS,
)

st.set_page_config(
    page_title="F&O Relative Volume Scanner",
    page_icon="📈",
    layout="wide",
)

st.title("📈 F&O Relative Volume Scanner")
st.caption("Today's volume till now vs Previous trading day volume till same clock time (IST)")

with st.expander("📘 Column & Momentum Guide — How to Use", expanded=False):
    st.markdown(COLUMN_GUIDE_MARKDOWN)


@st.cache_resource
def _run_startup_housekeeping() -> bool:
    """Prune stale/legacy cache files and warm holiday calendar asynchronously once per server lifecycle."""
    prune_cache()
    get_holidays(background_refresh=True)
    return True


_run_startup_housekeeping()

if "universe_symbols" not in st.session_state:
    syms, src = get_fno_universe(force_refresh=False)
    st.session_state.universe_symbols = syms
    st.session_state.universe_source = src

def _get_broker_secret(section: str, field: str, default: str = "") -> str:
    """Safely read broker config from st.secrets[section] or <SECTION>_<FIELD> env vars."""
    env_key = f"{section.upper()}_{field.upper()}"
    try:
        secrets_obj = getattr(st, "secrets", None)
        if secrets_obj is not None and section in secrets_obj:
            val = secrets_obj[section].get(field)
            if isinstance(val, str) and val.strip():
                return val.strip()
    except Exception:
        pass
    return (os.environ.get(env_key) or default).strip()


def _snapshot_key(interval: str, data_source: str) -> str:
    """Return isolated session snapshot cache key per interval and market data provider."""
    if data_source == "Upstox v3":
        return f"{interval}__upstox"
    if data_source == "5paisa Xstream":
        return f"{interval}__fivepaisa"
    return interval


# Session state initialization (keyed per snapshot_key so switching 1m/5m or Yahoo/Upstox preserves snapshots)
if "results_by_interval" not in st.session_state:
    st.session_state.results_by_interval = {}
if "scan_times_by_interval" not in st.session_state:
    st.session_state.scan_times_by_interval = {}
if "selected_columns" not in st.session_state:
    st.session_state.selected_columns = DEFAULT_COLUMNS.copy()
else:
    st.session_state.selected_columns = [
        c for c in st.session_state.selected_columns if c in ALL_COLUMNS
    ]
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
if "upstox_access_token" not in st.session_state:
    st.session_state.upstox_access_token = _get_broker_secret("upstox", "access_token")
if "fivepaisa_access_token" not in st.session_state:
    st.session_state.fivepaisa_access_token = _get_broker_secret("fivepaisa", "access_token")
if "fivepaisa_client_code" not in st.session_state:
    st.session_state.fivepaisa_client_code = _get_broker_secret("fivepaisa", "client_code")

# Sidebar controls
with st.sidebar:
    st.header("Settings")
    data_source_raw = st.segmented_control(
        "Data Source",
        options=["Yahoo Finance", "Upstox v3", "5paisa Xstream"],
        default="Yahoo Finance",
        selection_mode="single",
        key="data_source_choice",
        help="Use free Yahoo Finance (no login), Upstox v3, or 5paisa Xstream (daily token + real-time WebSocket stream).",
    )
    data_source = data_source_raw if data_source_raw in ("Upstox v3", "5paisa Xstream") else "Yahoo Finance"
    use_upstox = data_source == "Upstox v3"
    use_fivepaisa = data_source == "5paisa Xstream"

    upstox_token = ""
    upstox_live_ws = True
    fivepaisa_token = ""
    fivepaisa_client_code = ""
    fivepaisa_live_ws = True
    if use_upstox:
        upstox_token = st.text_input(
            "Upstox Access Token (Daily)",
            value=st.session_state.upstox_access_token,
            type="password",
            placeholder="Paste today's Upstox access_token",
            help="Upstox tokens expire daily. Paste today's token or use the OAuth Login Helper below.",
        ).strip()
        if upstox_token != st.session_state.upstox_access_token:
            st.session_state.upstox_access_token = upstox_token

        upstox_live_ws = st.checkbox(
            "Stream Live WSS (MarketDataStreamerV3)",
            value=True,
            help="Stream live full-quote ticks over WebSocket (zero REST polling once initialized).",
        )
        if not upstox_live_ws and upstox_token:
            stop_upstox_stream(upstox_token)

        with st.expander("🔑 Daily Login Helper (OAuth Code)", expanded=False):
            u_cid = st.text_input(
                "API Key (client_id)",
                value=_get_broker_secret("upstox", "client_id"),
                key="upstox_client_id",
            )
            u_csec = st.text_input(
                "API Secret",
                value=_get_broker_secret("upstox", "client_secret"),
                type="password",
                key="upstox_client_secret",
            )
            u_redir = st.text_input(
                "Redirect URI",
                value=_get_broker_secret("upstox", "redirect_uri", "http://localhost:8501"),
                key="upstox_redirect_uri",
            )
            if u_cid.strip() and u_redir.strip():
                login_url = build_upstox_login_url(u_cid, u_redir)
                st.markdown(f"[Open Upstox Daily Login]({login_url})")
            u_code = st.text_input(
                "Auth Code (from redirect ?code=...)",
                value="",
                key="upstox_auth_code",
            )
            if st.button("Exchange Code for Token", width="stretch"):
                if not (u_code.strip() and u_cid.strip() and u_csec.strip() and u_redir.strip()):
                    st.warning("Fill API Key, API Secret, Redirect URI, and Auth Code.")
                else:
                    tok, err = exchange_upstox_auth_code(u_code, u_cid, u_csec, u_redir)
                    if tok:
                        st.session_state.upstox_access_token = tok
                        upstox_token = tok
                        st.success("Upstox token saved for today's session.")
                    else:
                        st.error(err or "Upstox token exchange failed.")
    elif use_fivepaisa:
        fivepaisa_token = st.text_input(
            "5paisa Access Token (Daily)",
            value=st.session_state.fivepaisa_access_token,
            type="password",
            placeholder="Paste today's JWT access_token or use TOTP Login below",
            help="5paisa JWT access tokens expire daily. Generate one via TOTP/PIN or OAuth below.",
        ).strip()
        if fivepaisa_token != st.session_state.fivepaisa_access_token:
            st.session_state.fivepaisa_access_token = fivepaisa_token

        fivepaisa_client_code = st.text_input(
            "5paisa Client Code",
            value=st.session_state.fivepaisa_client_code,
            placeholder="e.g. 50011110",
            help="Required for MarketFeedV3 WebSocket subscriptions (auto-inferred from JWT if omitted).",
        ).strip()
        if fivepaisa_client_code != st.session_state.fivepaisa_client_code:
            st.session_state.fivepaisa_client_code = fivepaisa_client_code

        fivepaisa_live_ws = st.checkbox(
            "Stream Live WSS (MarketFeedV3)",
            value=True,
            help="Stream live NSE MarketFeedV3 ticks over WebSocket (up to 3,000 scrips, 50/batch).",
        )
        if not fivepaisa_live_ws and fivepaisa_token:
            stop_fivepaisa_stream(fivepaisa_token, fivepaisa_client_code)

        with st.expander("🔑 Daily Login Helper (TOTP / OAuth)", expanded=False):
            fp_user_key = st.text_input(
                "User Key (VendorKey)",
                value=_get_broker_secret("fivepaisa", "user_key"),
                key="fp_user_key",
            )
            fp_enc_key = st.text_input(
                "Encryption Key",
                value=_get_broker_secret("fivepaisa", "encryption_key"),
                type="password",
                key="fp_enc_key",
            )
            fp_user_id = st.text_input(
                "User ID",
                value=_get_broker_secret("fivepaisa", "user_id"),
                key="fp_user_id",
            )
            tab_totp, tab_oauth = st.tabs(["TOTP + PIN", "OAuth Redirect"])
            with tab_totp:
                fp_cc = st.text_input(
                    "Client Code",
                    value=fivepaisa_client_code or _get_broker_secret("fivepaisa", "client_code"),
                    key="fp_totp_client_code",
                )
                fp_pin = st.text_input(
                    "Trading PIN",
                    value=_get_broker_secret("fivepaisa", "pin"),
                    type="password",
                    key="fp_totp_pin",
                )
                fp_totp_val = st.text_input(
                    "TOTP (6-digit code or Base32 Secret)",
                    value=_get_broker_secret("fivepaisa", "totp_secret"),
                    type="password",
                    key="fp_totp_secret_or_code",
                )
                if st.button("Login via TOTP & Save Token", width="stretch"):
                    if not (fp_cc.strip() and fp_pin.strip() and fp_totp_val.strip() and fp_user_key.strip() and fp_enc_key.strip() and fp_user_id.strip()):
                        st.warning("Fill User Key, Encryption Key, User ID, Client Code, PIN, and TOTP.")
                    else:
                        tok, ccode, err = login_fivepaisa_totp(
                            fp_cc, fp_totp_val, fp_pin, fp_user_key, fp_enc_key, fp_user_id
                        )
                        if tok:
                            st.session_state.fivepaisa_access_token = tok
                            fivepaisa_token = tok
                            if ccode:
                                st.session_state.fivepaisa_client_code = ccode
                                fivepaisa_client_code = ccode
                            st.success("5paisa token generated via TOTP for today's session.")
                        else:
                            st.error(err or "5paisa TOTP login failed.")
            with tab_oauth:
                fp_redir = st.text_input(
                    "Response URL",
                    value=_get_broker_secret("fivepaisa", "redirect_uri", "http://localhost:8501"),
                    key="fp_redirect_uri",
                )
                if fp_user_key.strip() and fp_redir.strip():
                    fp_login_url = build_fivepaisa_login_url(fp_user_key, fp_redir)
                    st.markdown(f"[Open 5paisa Daily Login]({fp_login_url})")
                fp_req_tok = st.text_input(
                    "RequestToken (from callback URL)",
                    value="",
                    key="fp_request_token",
                )
                if st.button("Exchange RequestToken", width="stretch"):
                    if not (fp_req_tok.strip() and fp_user_key.strip() and fp_enc_key.strip() and fp_user_id.strip()):
                        st.warning("Fill User Key, Encryption Key, User ID, and RequestToken.")
                    else:
                        tok, ccode, err = exchange_fivepaisa_request_token(
                            fp_req_tok, fp_user_key, fp_enc_key, fp_user_id
                        )
                        if tok:
                            st.session_state.fivepaisa_access_token = tok
                            fivepaisa_token = tok
                            if ccode:
                                st.session_state.fivepaisa_client_code = ccode
                                fivepaisa_client_code = ccode
                            st.success("5paisa token saved for today's session.")
                        else:
                            st.error(err or "5paisa RequestToken exchange failed.")
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
    max_workers = st.slider(
        "Parallel download threads",
        4,
        16,
        8,
        help=(
            "Fallback yfinance thread count (default 8). The primary HTTP/2 async chart fetcher "
            "automatically runs 3× this value concurrently, capped at 28 (default 8 → 24 async streams)."
        ),
    )
    include_oi = st.checkbox(
        "Include NSE OI",
        value=True,
        help="Fetch aggregate F&O Open Interest & Day OI Change from NSE (1 rate-limited request every >=45s with backoff).",
    )
    min_oi_chg_pct = None
    oi_buildup_filter: list[str] = []
    if include_oi:
        min_oi_chg_pct = st.number_input(
            "Min OI Chg % (Day)",
            min_value=-100.0,
            max_value=1000.0,
            value=None,
            step=1.0,
            placeholder="Any (e.g. 5.0)",
            help="Show stocks with Day OI Chg % >= threshold (leave blank for all).",
        )
        oi_buildup_filter = st.multiselect(
            "OI Buildup (Price Δ vs OI Δ)",
            options=list(OI_BUILDUP_OPTIONS),
            default=[],
            placeholder="All patterns",
            help="Filter by intraday Price Change vs Day OI Change pattern.",
        )

    available_sorts = [
        label
        for label, (col, _, _) in SORT_OPTIONS.items()
        if include_oi or (not col.startswith("OI") and col != "Signal")
    ]
    sort_by = st.selectbox(
        "Sort results by",
        options=available_sorts,
        index=0,
        help="Sort scanned results immediately in memory.",
    )

    use_momentum_filter = st.checkbox(
        "Momentum Quality Filters",
        value=False,
        help="Filter out faded spikes, weak day-range closes, red-candle gap-ups, and dead morning volume.",
    )
    momentum_filter_state = None
    if use_momentum_filter:
        require_vwap_aligned = st.checkbox(
            "VWAP aligned (+ above / - below)",
            value=True,
            help="Require LTP > VWAP for gainers (% Chg > 0) and LTP < VWAP for losers (% Chg < 0).",
        )
        require_open_aligned = st.checkbox(
            "Intraday candle aligned (From Open %)",
            value=True,
            help="Require LTP > Day Open for gainers and LTP < Day Open for losers (filters faded gap-ups/downs).",
        )
        range_edge_pct = st.slider(
            "Hold within % of Day High/Low",
            10,
            50,
            30,
            help="Require Range Pos >= (100 - X)% for gainers or <= X% for losers (e.g. 30 = top/bottom 30%).",
        )
        min_vol_burst_15m = st.number_input(
            "Min 15m Vol Burst (×)",
            min_value=0.0,
            max_value=20.0,
            value=1.2,
            step=0.1,
            help="Require recent 15m volume pace >= X times today's average pace (0 disables).",
        )
        momentum_filter_state = (
            bool(require_vwap_aligned),
            bool(require_open_aligned),
            int(range_edge_pct),
            float(min_vol_burst_15m),
        )

    st.markdown("---")
    live_ws_active = (use_upstox and upstox_live_ws) or (use_fivepaisa and fivepaisa_live_ws)
    auto_refresh = st.checkbox("Auto-refresh", value=False)
    refresh_seconds = 10 if live_ws_active else 60
    if auto_refresh:
        refresh_opts = [5, 10, 30, 60, 120, 300] if live_ws_active else [30, 60, 120, 300]
        refresh_seconds = st.selectbox(
            "Auto-refresh interval",
            options=refresh_opts,
            index=1,
            format_func=lambda s: f"{s} seconds" if s < 60 else f"{s // 60} minute{'s' if s > 60 else ''}",
            help="Interval between automated background updates while NSE is open.",
        )
    st.markdown("---")
    if st.button("🔄 Sync NSE F&O List", width="stretch"):
        with st.spinner("Fetching latest F&O list and trading calendar from NSE..."):
            with ThreadPoolExecutor(max_workers=2) as pool:
                fut_universe = pool.submit(get_fno_universe, True)
                fut_holidays = pool.submit(get_holidays, True)
                syms, src = fut_universe.result()
                fut_holidays.result()
            st.session_state.universe_symbols = syms
            st.session_state.universe_source = src
            st.session_state._cached_prev_day = None
            st.session_state._skip_auto_scan_once = True

    active_universe = st.session_state.universe_symbols
    st.info(
        f"Universe size: **{len(active_universe)}** F&O stocks\n\n"
        f"Source: {st.session_state.universe_source}"
    )

snapshot_key = _snapshot_key(interval, data_source)

# Detect sidebar filter/config tweaks so changing filters never hijacks into a network scan
_source_state = False if data_source == "Yahoo Finance" else (True if use_upstox else "fivepaisa")
current_filter_state = (
    float(min_ratio),
    int(min_volume),
    bool(exclude_gap_up),
    bool(exclude_gap_down),
    float(gap_threshold),
    int(max_workers),
    bool(include_oi),
    _source_state,
    None if min_oi_chg_pct is None else float(min_oi_chg_pct),
    tuple(oi_buildup_filter),
    sort_by,
    momentum_filter_state,
)
if (
    st.session_state._last_filter_state is not None
    and st.session_state._last_filter_state != current_filter_state
):
    st.session_state._skip_auto_scan_once = True
elif (
    st.session_state._last_interval is not None
    and st.session_state._last_interval != snapshot_key
    and snapshot_key in st.session_state.scan_times_by_interval
):
    st.session_state._skip_auto_scan_once = True
st.session_state._last_filter_state = current_filter_state
st.session_state._last_interval = snapshot_key


def _mark_ui_interaction() -> None:
    st.session_state._skip_auto_scan_once = True


def _reset_default_columns() -> None:
    st.session_state.selected_columns = DEFAULT_COLUMNS.copy()
    st.session_state._skip_auto_scan_once = True


def _filter_and_sort_results(
    raw_df: pd.DataFrame,
    min_ratio: float,
    min_volume: int,
    exclude_gap_up: bool,
    exclude_gap_down: bool,
    gap_threshold: float,
    include_oi: bool,
    min_oi_chg_pct: float | None,
    oi_buildup_filter: tuple[str, ...],
    sort_by: str,
    momentum_filter_state: tuple[bool, bool, int, float] | None = None,
) -> tuple[pd.DataFrame, bool, int, bool, int, bool, int]:
    return filter_and_sort_results(
        raw_df=raw_df,
        min_ratio=min_ratio,
        min_volume=min_volume,
        exclude_gap_up=exclude_gap_up,
        exclude_gap_down=exclude_gap_down,
        gap_threshold=gap_threshold,
        include_oi=include_oi,
        min_oi_chg_pct=min_oi_chg_pct,
        oi_buildup_filter=oi_buildup_filter,
        sort_by=sort_by,
        sort_options=SORT_OPTIONS,
        momentum_filter_state=momentum_filter_state,
    )


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
    include_oi: bool = True,
    min_oi_chg_pct: float | None = None,
    oi_buildup_filter: tuple[str, ...] = (),
    sort_by: str = "Ratio (High → Low)",
    momentum_filter_state: tuple[bool, bool, int, float] | None = None,
    data_source: str = "Yahoo Finance",
    upstox_token: str = "",
    upstox_live_ws: bool = True,
    fivepaisa_token: str = "",
    fivepaisa_client_code: str = "",
    fivepaisa_live_ws: bool = True,
):
    now = datetime.now(IST)
    holidays_set = get_holidays(background_refresh=True)
    market_open = is_market_open(now, background_refresh=True)
    snapshot_key = _snapshot_key(interval, data_source)

    last_scan_dt = st.session_state.scan_times_by_interval.get(snapshot_key)
    last_scan_started_dt = st.session_state._scan_started_by_interval.get(snapshot_key)
    skip_once = bool(st.session_state._skip_auto_scan_once)
    st.session_state._skip_auto_scan_once = False

    attempt_rec = st.session_state._post_close_attempts_by_interval.get(snapshot_key)
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
            with st.spinner(f"Scanning {len(symbols)} F&O stocks ({data_source}, {interval} bars)..."):
                scan_started_dt = datetime.now(IST)
                scan_kwargs = {
                    "symbols": symbols,
                    "min_ratio": 0.0,
                    "min_volume": 0,
                    "interval": interval,
                    "max_workers": max_workers,
                    "prev_day": prev_day,
                    "include_oi": include_oi,
                }
                if data_source == "Upstox v3":
                    scan_kwargs["provider"] = UpstoxProvider(
                        access_token=upstox_token,
                        use_live_ws=upstox_live_ws,
                    )
                elif data_source == "5paisa Xstream":
                    scan_kwargs["provider"] = FivePaisaProvider(
                        access_token=fivepaisa_token,
                        client_code=fivepaisa_client_code,
                        use_live_ws=fivepaisa_live_ws,
                    )
                raw_df = run_scan(**scan_kwargs)
                if not raw_df.empty:
                    st.session_state.results_by_interval[snapshot_key] = raw_df
                    last_scan_dt = datetime.now(IST)
                    st.session_state.scan_times_by_interval[snapshot_key] = last_scan_dt
                    st.session_state._scan_started_by_interval[snapshot_key] = scan_started_dt
                    st.session_state._last_scan_empty_by_interval.pop(snapshot_key, None)
                    if not market_open:
                        st.session_state._post_close_attempts_by_interval.pop(snapshot_key, None)
                else:
                    st.session_state._last_scan_empty_by_interval[snapshot_key] = datetime.now(IST)
                    empty_meta = raw_df.attrs.get("provider_meta")
                    if isinstance(empty_meta, dict):
                        st.session_state[f"_last_provider_meta_{snapshot_key}"] = empty_meta
                    if not scan_clicked and not market_open and now.time() > MARKET_CLOSE_TIME:
                        st.session_state._post_close_attempts_by_interval[snapshot_key] = (
                            now.date(),
                            post_close_attempts + 1,
                        )

    if auto_refresh:
        now_after = datetime.now(IST)
        if is_market_open(now_after, background_refresh=True):
            info_slot.caption(
                f"⏱️ **Auto-refresh active** (every {refresh_seconds}s). "
                "Updating in the background without full-page reloads."
            )
        else:
            info_slot.caption("⏸️ **Auto-refresh paused** (market closed). Manual **Scan Now** still works anytime.")

    # Client-side filtering & sorting on the cached scan snapshot for the active source & interval
    raw_df = st.session_state.results_by_interval.get(snapshot_key, pd.DataFrame())
    raw_empty_dt = st.session_state._last_scan_empty_by_interval.get(snapshot_key)
    empty_scan_dt = raw_empty_dt if (raw_empty_dt is not None and raw_empty_dt.date() == now.date()) else None
    df, gap_missing, gap_excluded_count, oi_missing, oi_excluded_count, momentum_missing, momentum_excluded_count = _filter_and_sort_results(
        raw_df=raw_df,
        min_ratio=min_ratio,
        min_volume=min_volume,
        exclude_gap_up=exclude_gap_up,
        exclude_gap_down=exclude_gap_down,
        gap_threshold=gap_threshold,
        include_oi=include_oi,
        min_oi_chg_pct=min_oi_chg_pct,
        oi_buildup_filter=oi_buildup_filter,
        sort_by=sort_by,
        momentum_filter_state=momentum_filter_state,
    )

    has_signals = include_oi and (not df.empty) and ("Signal" in df.columns)
    table_df = df
    sig_choice = "All"

    with results_slot:
        if df.empty:
            if last_scan_dt is None and empty_scan_dt is None:
                st.info("Click **Scan Now** or enable **Auto-refresh** in the sidebar to run the scan.")
            elif not raw_df.empty:
                st.warning("No stocks matched the current filters. Try lowering the Minimum Ratio or Volume.")
        else:
            if has_signals:
                bullish_count = int((df["Signal"] == SIGNAL_BULLISH).sum())
                bearish_count = int((df["Signal"] == SIGNAL_BEARISH).sum())
                total_count = len(df)
                col_sig_summary, col_sig_pills = st.columns([3, 2], vertical_alignment="center")
                with col_sig_summary:
                    st.markdown(
                        f"**Signals:** &nbsp;{SIGNAL_BULLISH}: **{bullish_count}** &nbsp;|&nbsp; "
                        f"{SIGNAL_BEARISH}: **{bearish_count}** &nbsp;|&nbsp; **All:** {total_count}"
                    )
                    st.caption("Highlight rule: Ratio > 1.0×, OI Chg > +10%, % Chg > +1.5% / < -1.5%")
                with col_sig_pills:
                    sig_choice = (
                        st.segmented_control(
                            "Quick Signal Filter",
                            options=["All", SIGNAL_BULLISH, SIGNAL_BEARISH],
                            default="All",
                            selection_mode="single",
                            label_visibility="collapsed",
                            key="quick_signal_filter",
                            on_change=_mark_ui_interaction,
                        )
                        or "All"
                    )
                if sig_choice in (SIGNAL_BULLISH, SIGNAL_BEARISH):
                    table_df = df[df["Signal"] == sig_choice].reset_index(drop=True)

            if table_df.empty:
                st.info(f"No **{sig_choice}** stocks matched the current scan and filters.")
            else:
                st.session_state.selected_columns = [
                    c for c in st.session_state.selected_columns if c in ALL_COLUMNS
                ]
                with st.popover("⚙️ Customize Columns"):
                    st.multiselect(
                        "Visible columns (add, remove, or reorder)",
                        options=ALL_COLUMNS,
                        format_func=lambda col: COLUMN_LABELS.get(col, col),
                        key="selected_columns",
                        on_change=_mark_ui_interaction,
                    )
                    st.button("Reset to Default", on_click=_reset_default_columns)

                visible_cols = [c for c in st.session_state.selected_columns if c in table_df.columns]
                if not visible_cols:
                    st.warning("Select at least one column in **Customize Columns** to display results.")
                else:
                    display_df = table_df[visible_cols]
                    active_config = {c: COLUMN_CONFIG[c] for c in visible_cols if c in COLUMN_CONFIG}
                    table_data = (
                        _style_scanner_df(display_df, table_df["Signal"])
                        if has_signals
                        else display_df
                    )

                    st.dataframe(
                        table_data,
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

    with status_slot:
        if last_scan_dt is not None:
            gap_note = ""
            if (exclude_gap_up or exclude_gap_down) and not gap_missing:
                gap_note = f" | Excluded by gap filter: **{gap_excluded_count}**"
            oi_filter_note = ""
            if include_oi and (min_oi_chg_pct is not None or oi_buildup_filter) and not oi_missing:
                oi_filter_note = f" | Filtered by OI: **{oi_excluded_count}**"
            mom_filter_note = ""
            if momentum_filter_state is not None and not momentum_missing:
                mom_filter_note = f" | Filtered by momentum: **{momentum_excluded_count}**"
            oi_meta = raw_df.attrs.get("oi_meta") if include_oi else None
            oi_note = ""
            if isinstance(oi_meta, dict):
                nse_ts = oi_meta.get("nse_timestamp")
                if nse_ts:
                    oi_note = f" | NSE OI @ {nse_ts.split(' ')[-1]}"
            prov_meta = raw_df.attrs.get("provider_meta")
            prov_note = ""
            prov_labels = {"upstox": "Upstox", "fivepaisa": "5paisa"}
            prov_id = prov_meta.get("provider") if isinstance(prov_meta, dict) else None
            if prov_id in prov_labels:
                p_name = prov_labels[prov_id]
                mode = prov_meta.get("mode")
                if mode == "wss":
                    ws_badge = "🟢 WSS" if prov_meta.get("ws_connected") else "🟡 WSS"
                    prov_note = f" | {p_name} {ws_badge} ({prov_meta.get('ticks', 0)} ticks)"
                elif mode == "batch_quote":
                    prov_note = f" | {p_name} ⚡ Batch Quote"
                elif mode == "rest_seed":
                    prov_note = f" | {p_name} Seeded REST"
                else:
                    prov_note = f" | {p_name} REST"
            if sig_choice in (SIGNAL_BULLISH, SIGNAL_BEARISH):
                count_summary = (
                    f"Showing **{len(table_df)}** {sig_choice} of **{len(df)}** matching "
                    f"(**{len(raw_df)}** scanned)"
                )
            else:
                count_summary = f"Showing **{len(df)}** of **{len(raw_df)}** scanned stocks"
            st.success(
                f"Last scan: {last_scan_dt.strftime('%H:%M:%S IST')} "
                f"({interval} bars) | "
                f"{count_summary}{gap_note}{oi_filter_note}{mom_filter_note}{oi_note}{prov_note}"
            )
            if isinstance(prov_meta, dict) and prov_meta.get("error"):
                p_label = prov_labels.get(prov_id, data_source)
                st.warning(f"{p_label} notice: {prov_meta['error']}")
            if isinstance(oi_meta, dict) and oi_meta.get("error"):
                cd = int(round(oi_meta.get("cooldown_seconds") or 0))
                cd_note = f" (cooling down {cd}s)" if cd > 0 else ""
                fallback_msg = "using last cached OI snapshot" if oi_meta.get("nse_timestamp") else "OI unavailable"
                st.warning(f"NSE OI notice: {oi_meta['error']}{cd_note} — {fallback_msg}.")
            if empty_scan_dt is not None and not raw_df.empty:
                st.warning(
                    f"Latest scan at {empty_scan_dt.strftime('%H:%M:%S IST')} returned no data — "
                    "showing the previous snapshot."
                )
            if gap_missing:
                st.warning(
                    "Current results were scanned before Gap % data was added — click **Scan Now** once to apply the gap filter."
                )
            if oi_missing:
                st.warning(
                    "Current results were scanned without OI data — click **Scan Now** once to apply OI filters."
                )
            if momentum_missing:
                st.warning(
                    "Current results were scanned before VWAP / 15m Vol Burst metrics were added — click **Scan Now** once to apply all momentum filters."
                )
        elif empty_scan_dt is not None:
            last_prov_meta = st.session_state.get(f"_last_provider_meta_{snapshot_key}")
            if isinstance(last_prov_meta, dict) and last_prov_meta.get("error"):
                err_label = {"upstox": "Upstox", "fivepaisa": "5paisa"}.get(last_prov_meta.get("provider"), data_source)
                st.warning(f"{err_label} error: {last_prov_meta['error']}")
            else:
                st.warning(
                    f"Scan at {empty_scan_dt.strftime('%H:%M:%S IST')} returned no intraday data for today "
                    "(market may be pre-open, closed, or the data feed has no bars yet)."
                )
        elif st.session_state.results_by_interval:
            st.info(f"No **{data_source} ({interval})** snapshot yet — click **Scan Now** to fetch {interval} bars.")


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
    include_oi=include_oi,
    min_oi_chg_pct=None if min_oi_chg_pct is None else float(min_oi_chg_pct),
    oi_buildup_filter=tuple(oi_buildup_filter),
    sort_by=sort_by,
    momentum_filter_state=momentum_filter_state,
    data_source=data_source,
    upstox_token=upstox_token,
    upstox_live_ws=upstox_live_ws,
    fivepaisa_token=fivepaisa_token,
    fivepaisa_client_code=fivepaisa_client_code,
    fivepaisa_live_ws=fivepaisa_live_ws,
)

st.markdown("---")
st.caption(
    "Data sources: Yahoo Finance, Upstox v3, or 5paisa Xstream (intraday OHLCV / live WSS) & NSE India (F&O OI & holidays). "
    "For educational / research purposes only — not investment advice."
)
