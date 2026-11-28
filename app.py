"""
Relative Volume Scanner (F&O Stocks) - Streamlit Web App
Today's cumulative volume till now vs Previous day's volume till same time.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import html
import os
from typing import Any
import streamlit as st
import pandas as pd

from core.holidays import get_holidays, is_market_open, is_wss_active_window, get_previous_trading_day, IST
from core.oi import OI_BUILDUP_OPTIONS, SIGNAL_BEARISH, SIGNAL_BULLISH
from core.scanner import prune_cache, run_scan
from core.universe import get_fno_universe
from providers import (
    FivePaisaProvider,
    UpstoxProvider,
    build_fivepaisa_login_url,
    build_upstox_login_url,
    exchange_fivepaisa_request_token,
    exchange_upstox_auth_code,
    login_fivepaisa_totp,
    stop_fivepaisa_stream,
    stop_upstox_stream,
)
from providers.common.stream_base import enforce_market_hours_shutdown
from ui.auth_helpers import (
    detect_default_redirect_uri,
    extract_oauth_param,
    inspect_fivepaisa_jwt,
    is_local_host_request,
    load_local_broker_auth,
    pop_completed_oauth_token,
    pop_oauth_handoff,
    register_oauth_handoff,
    save_local_broker_auth,
    store_completed_oauth_token,
)
from ui.screener_filters import (
    MARKET_CLOSE_TIME,
    filter_and_sort_results,
    should_auto_scan as _should_auto_scan,
    style_scanner_df as _style_scanner_df,
)
from ui.table_config import (
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

st.markdown(
    """
    <style>
        [data-testid="stMainBlockContainer"] { padding-top: 2.2rem; padding-bottom: 1rem; }
        [data-testid="stSidebarUserContent"] { padding-top: 1.25rem; }
        [data-testid="stMetricLabel"] { margin-bottom: 0.1rem; }
        [data-testid="stMetricValue"] { font-size: 1.45rem; line-height: 1.2; }
        [data-testid="stSidebar"] [data-testid="stButtonGroup"] { flex-wrap: wrap; gap: 0.25rem; }
    </style>
    """,
    unsafe_allow_html=True,
)
st.title("📈 F&O Relative Volume Scanner")
st.caption("Today's cumulative volume vs previous trading day till the same clock time (IST)")


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

def _get_broker_secret(
    section: str,
    field: str,
    default: str = "",
    local_auth: dict[str, str] | None = None,
) -> str:
    """Safely read broker config from st.secrets, env vars, or optional local same-day store."""
    env_key = f"{section.upper()}_{field.upper()}"
    try:
        secrets_obj = getattr(st, "secrets", None)
        if secrets_obj is not None and section in secrets_obj:
            val = secrets_obj[section].get(field)
            if isinstance(val, str) and val.strip():
                return val.strip()
    except Exception:
        pass
    env_val = (os.environ.get(env_key) or "").strip()
    if env_val:
        return env_val
    if local_auth:
        local_val = (local_auth.get(field) or "").strip()
        if local_val:
            return local_val
    return default.strip()


def _detect_local_browser() -> bool:
    try:
        return is_local_host_request(st.context.headers)
    except Exception:
        return False


def _default_redirect_uri() -> str:
    try:
        ctx = st.context
        return detect_default_redirect_uri(
            context_url=getattr(ctx, "url", None),
            headers=getattr(ctx, "headers", None),
        )
    except Exception:
        return "http://localhost:8501"


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
if "_local_persist_enabled" not in st.session_state:
    st.session_state._local_persist_enabled = _detect_local_browser()
if "_upstox_cleared_explicitly" not in st.session_state:
    st.session_state._upstox_cleared_explicitly = False
if "_fivepaisa_cleared_explicitly" not in st.session_state:
    st.session_state._fivepaisa_cleared_explicitly = False

_need_local_auth_init = (
    st.session_state._local_persist_enabled
    and (
        "upstox_access_token" not in st.session_state
        or "fivepaisa_access_token" not in st.session_state
        or (
            st.session_state.get("data_source_choice") == "Upstox v3"
            and not st.session_state.get("upstox_access_token")
            and not st.session_state._upstox_cleared_explicitly
        )
        or (
            st.session_state.get("data_source_choice") == "5paisa Xstream"
            and not st.session_state.get("fivepaisa_access_token")
            and not st.session_state._fivepaisa_cleared_explicitly
        )
    )
)
_init_local_up = (
    load_local_broker_auth("upstox")
    if (_need_local_auth_init and not st.session_state._upstox_cleared_explicitly)
    else {}
)
_init_local_fp = (
    load_local_broker_auth("fivepaisa")
    if (_need_local_auth_init and not st.session_state._fivepaisa_cleared_explicitly)
    else {}
)

if "upstox_access_token" not in st.session_state:
    st.session_state.upstox_access_token = _get_broker_secret("upstox", "access_token", local_auth=_init_local_up)
elif (
    st.session_state._local_persist_enabled
    and not st.session_state.upstox_access_token
    and not st.session_state._upstox_cleared_explicitly
    and _init_local_up.get("access_token")
):
    st.session_state.upstox_access_token = _init_local_up["access_token"]
    st.session_state._w_upstox_token = _init_local_up["access_token"]

if "fivepaisa_access_token" not in st.session_state:
    st.session_state.fivepaisa_access_token = _get_broker_secret("fivepaisa", "access_token", local_auth=_init_local_fp)
elif (
    st.session_state._local_persist_enabled
    and not st.session_state.fivepaisa_access_token
    and not st.session_state._fivepaisa_cleared_explicitly
    and _init_local_fp.get("access_token")
):
    st.session_state.fivepaisa_access_token = _init_local_fp["access_token"]
    st.session_state._w_fivepaisa_token = _init_local_fp["access_token"]

if "fivepaisa_client_code" not in st.session_state:
    st.session_state.fivepaisa_client_code = _get_broker_secret("fivepaisa", "client_code", local_auth=_init_local_fp)
if "data_source_choice" not in st.session_state:
    st.session_state.data_source_choice = "Yahoo Finance"

_detected_redirect_uri = _default_redirect_uri()

for _sk, _wk, _sec, _fld, _dft, _loc in (
    ("_u_client_id", "_w_u_client_id", "upstox", "client_id", "", _init_local_up),
    ("_u_client_secret", "_w_u_client_secret", "upstox", "client_secret", "", _init_local_up),
    ("_u_redirect_uri", "_w_u_redirect_uri", "upstox", "redirect_uri", _detected_redirect_uri, _init_local_up),
    ("_u_auth_code", "_w_u_auth_code", "", "", "", None),
    ("_fp_user_key", "_w_fp_user_key", "fivepaisa", "user_key", "", _init_local_fp),
    ("_fp_enc_key", "_w_fp_enc_key", "fivepaisa", "encryption_key", "", _init_local_fp),
    ("_fp_user_id", "_w_fp_user_id", "fivepaisa", "user_id", "", _init_local_fp),
    ("_fp_redirect_uri", "_w_fp_redirect_uri", "fivepaisa", "redirect_uri", _detected_redirect_uri, _init_local_fp),
    ("_fp_totp_pin", "_w_fp_totp_pin", "fivepaisa", "pin", "", None),
    ("_fp_totp_secret", "_w_fp_totp_secret", "fivepaisa", "totp_secret", "", None),
    ("_fp_req_token", "_w_fp_req_token", "", "", "", None),
):
    _explicit_sec = _get_broker_secret(_sec, _fld, "") if _sec else ""
    _snap_key = f"_secret_snap_{_sk}"
    if _sk not in st.session_state:
        st.session_state[_snap_key] = _explicit_sec
        st.session_state[_sk] = _explicit_sec or (
            _get_broker_secret(_sec, _fld, _dft, local_auth=_loc) if _sec else _dft
        )
    elif _sec and _explicit_sec and _explicit_sec != st.session_state.get(_snap_key, ""):
        st.session_state[_snap_key] = _explicit_sec
        st.session_state[_sk] = _explicit_sec
        st.session_state[_wk] = _explicit_sec


def _consume_origin_tab_oauth(provider: str, nonce: str) -> bool:
    """Pull a completed cross-tab OAuth token into pending session updates if ready."""
    if not nonce:
        return False
    done = pop_completed_oauth_token(nonce)
    if not done or not done.get("access_token"):
        return False
    if provider == "upstox":
        st.session_state._pending_upstox_token = done["access_token"]
        st.session_state._upstox_login_notice = "Upstox token synced automatically from browser login."
        st.session_state.pop("_u_oauth_nonce", None)
        st.session_state.pop("_u_oauth_snap", None)
    else:
        st.session_state._pending_fivepaisa_auth = (done["access_token"], done.get("client_code", ""))
        st.session_state._fivepaisa_login_notice = "5paisa token synced automatically from browser login."
        st.session_state.pop("_fp_oauth_nonce", None)
        st.session_state.pop("_fp_oauth_snap", None)
    return True


def _save_local_if_changed(provider: str, payload: dict[str, str]) -> None:
    """Persist local broker fields only when their trimmed values differ from last synced state."""
    snap_key = f"_local_save_snap_{provider}"
    frozen = tuple(sorted((k, str(v or "").strip()) for k, v in payload.items()))
    if st.session_state.get(snap_key) == frozen:
        return
    save_local_broker_auth(provider, payload)
    st.session_state[snap_key] = frozen


def _ensure_oauth_link_state(payload: dict[str, str], nonce_key: str, snap_key: str) -> str:
    """Reuse existing OAuth handoff nonce while credentials remain unchanged to avoid lock churn."""
    cur_nonce = st.session_state.get(nonce_key, "")
    frozen = tuple(sorted((k, str(v or "").strip()) for k, v in payload.items()))
    if cur_nonce and st.session_state.get(snap_key) == frozen:
        return cur_nonce
    new_nonce = register_oauth_handoff(payload, nonce=cur_nonce)
    st.session_state[nonce_key] = new_nonce
    st.session_state[snap_key] = frozen
    return new_nonce


def _apply_pending_auth_updates() -> None:
    """Apply token/code updates from previous button clicks or cross-tab OAuth sync before sidebar widgets mount."""
    if not st.session_state.get("upstox_access_token") and st.session_state.get("_u_oauth_nonce"):
        _consume_origin_tab_oauth("upstox", st.session_state["_u_oauth_nonce"])
    if not st.session_state.get("fivepaisa_access_token") and st.session_state.get("_fp_oauth_nonce"):
        _consume_origin_tab_oauth("fivepaisa", st.session_state["_fp_oauth_nonce"])

    if "_pending_upstox_token" in st.session_state:
        tok_val = str(st.session_state.pop("_pending_upstox_token") or "").strip()
        st.session_state.upstox_access_token = tok_val
        st.session_state._w_upstox_token = tok_val
        st.session_state._upstox_cleared_explicitly = not bool(tok_val)
        st.session_state._u_auth_code = ""
        st.session_state._w_u_auth_code = ""

    if "_pending_fivepaisa_auth" in st.session_state:
        fp_tok, fp_cc = st.session_state.pop("_pending_fivepaisa_auth")
        clean_tok = str(fp_tok or "").strip()
        clean_cc = str(fp_cc or "").strip()
        st.session_state.fivepaisa_access_token = clean_tok
        st.session_state._w_fivepaisa_token = clean_tok
        st.session_state._fivepaisa_cleared_explicitly = not bool(clean_tok)
        if clean_cc:
            st.session_state.fivepaisa_client_code = clean_cc
            st.session_state._w_fivepaisa_client_code = clean_cc
            st.session_state._w_fp_totp_cc = clean_cc
        st.session_state._fp_req_token = ""
        st.session_state._w_fp_req_token = ""


def _capture_oauth_redirect_params() -> None:
    """Capture ?code= (Upstox) or ?RequestToken= (5paisa) and optional state handoff from OAuth callbacks."""
    qp = getattr(st, "query_params", None)
    if qp is None:
        return
    try:
        keys = list(qp.keys())
    except Exception:
        return
    if not keys or not all(isinstance(k, str) for k in keys):
        return

    def _pop_qp(*names: str) -> str:
        found = ""
        lowered = {k.lower(): k for k in list(qp.keys()) if isinstance(k, str)}
        for name in names:
            actual = name if name in lowered.values() else lowered.get(name.lower())
            if not actual:
                continue
            raw = qp.get(actual)
            if isinstance(raw, list):
                raw = raw[0] if raw else ""
            if isinstance(raw, str) and raw.strip() and not found:
                found = raw.strip()
            try:
                del qp[actual]
            except Exception:
                pass
        return found

    up_code = _pop_qp("code")
    if up_code:
        oauth_state = _pop_qp("state")
        if oauth_state:
            st.session_state._active_up_oauth_state = oauth_state
        handoff = pop_oauth_handoff(oauth_state)
        if handoff:
            for dst_state, dst_widget, src_key in (
                ("_u_client_id", "_w_u_client_id", "client_id"),
                ("_u_client_secret", "_w_u_client_secret", "client_secret"),
                ("_u_redirect_uri", "_w_u_redirect_uri", "redirect_uri"),
            ):
                if handoff.get(src_key):
                    st.session_state[dst_state] = handoff[src_key]
                    st.session_state[dst_widget] = handoff[src_key]
        extracted = extract_oauth_param(up_code, ("code",))
        st.session_state._u_auth_code = extracted
        st.session_state._w_u_auth_code = extracted
        st.session_state.data_source_choice = "Upstox v3"
        st.session_state._auto_exchange_upstox = True
        for extra in ("error", "error_description"):
            _pop_qp(extra)
        return

    fp_req = _pop_qp("RequestToken", "request_token", "requesttoken")
    if fp_req:
        oauth_state = _pop_qp("state")
        if oauth_state:
            st.session_state._active_fp_oauth_state = oauth_state
        handoff = pop_oauth_handoff(oauth_state)
        if handoff:
            for dst_state, dst_widget, src_key in (
                ("_fp_user_key", "_w_fp_user_key", "user_key"),
                ("_fp_enc_key", "_w_fp_enc_key", "encryption_key"),
                ("_fp_user_id", "_w_fp_user_id", "user_id"),
                ("_fp_redirect_uri", "_w_fp_redirect_uri", "redirect_uri"),
            ):
                if handoff.get(src_key):
                    st.session_state[dst_state] = handoff[src_key]
                    st.session_state[dst_widget] = handoff[src_key]
        extracted = extract_oauth_param(fp_req, ("RequestToken", "requesttoken", "request_token"))
        st.session_state._fp_req_token = extracted
        st.session_state._w_fp_req_token = extracted
        fp_cc = _pop_qp("ClientCode", "clientcode", "client_code")
        if fp_cc and not st.session_state.get("fivepaisa_client_code"):
            st.session_state.fivepaisa_client_code = fp_cc
            st.session_state._w_fivepaisa_client_code = fp_cc
        st.session_state.data_source_choice = "5paisa Xstream"
        st.session_state._auto_exchange_fivepaisa = True
        for extra in ("error", "error_description"):
            _pop_qp(extra)
        return

    oauth_err = _pop_qp("error_description", "error")
    if oauth_err:
        st.session_state._oauth_error_notice = f"OAuth login error: {oauth_err}"


def _sync_text_field(label: str, state_key: str, widget_key: str, **kwargs: Any) -> str:
    """Render a text_input backed by durable session_state across sidebar view changes."""
    if widget_key not in st.session_state:
        st.session_state[widget_key] = st.session_state.get(state_key, "")
    val = st.text_input(label, key=widget_key, **kwargs).strip()
    st.session_state[state_key] = val
    return val


def _clear_upstox_token() -> None:
    tok = st.session_state.get("upstox_access_token", "")
    if tok:
        stop_upstox_stream(tok)
    st.session_state.upstox_access_token = ""
    st.session_state._w_upstox_token = ""
    st.session_state._upstox_cleared_explicitly = True
    st.session_state.pop("_upstox_login_notice", None)
    st.session_state.pop("_u_oauth_nonce", None)
    st.session_state.pop("_u_oauth_snap", None)
    st.session_state.pop("_local_save_snap_upstox", None)
    if st.session_state.get("_local_persist_enabled"):
        save_local_broker_auth("upstox", {}, clear_token=True)


def _clear_fivepaisa_token() -> None:
    tok = st.session_state.get("fivepaisa_access_token", "")
    ccode = st.session_state.get("fivepaisa_client_code", "")
    if tok:
        stop_fivepaisa_stream(tok, ccode)
    st.session_state.fivepaisa_access_token = ""
    st.session_state._w_fivepaisa_token = ""
    st.session_state._fivepaisa_cleared_explicitly = True
    st.session_state.pop("_fivepaisa_login_notice", None)
    st.session_state.pop("_fp_oauth_nonce", None)
    st.session_state.pop("_fp_oauth_snap", None)
    st.session_state.pop("_local_save_snap_fivepaisa", None)
    if st.session_state.get("_local_persist_enabled"):
        save_local_broker_auth("fivepaisa", {}, clear_token=True)


def _render_same_tab_login_link(label: str, url: str, link_id: str) -> None:
    """Render a full-width button-styled link targeting _top so OAuth login stays in one tab."""
    safe_id = html.escape(link_id, quote=True)
    safe_url = html.escape(url, quote=True)
    safe_label = html.escape(label, quote=False)
    st.html(
        f"""
        <style>
          #{safe_id} {{
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 100%;
            box-sizing: border-box;
            min-height: 2.4rem;
            padding: 0.4rem 0.75rem;
            border-radius: 0.5rem;
            border: 1px solid rgba(128, 128, 128, 0.35);
            color: inherit;
            text-decoration: none;
            font-size: 0.875rem;
            font-weight: 400;
            line-height: 1.4;
            transition: border-color 0.15s ease, background-color 0.15s ease;
          }}
          #{safe_id}:hover {{
            border-color: rgb(255, 75, 75);
            color: rgb(255, 75, 75);
            background-color: rgba(255, 75, 75, 0.06);
          }}
        </style>
        <a id="{safe_id}" href="{safe_url}">{safe_label}</a>
        <script>
          (function() {{
            var el = document.getElementById("{safe_id}");
            if (el) {{ el.setAttribute("target", "_top"); }}
          }})();
        </script>
        """,
        unsafe_allow_javascript=True,
    )


def _render_local_persist_toggle() -> bool:
    """Render persistent storage toggle (auto-enabled on localhost/LAN, off on public cloud)."""
    if "_w_remember_local" not in st.session_state:
        st.session_state._w_remember_local = bool(st.session_state._local_persist_enabled)
    checked = bool(
        st.checkbox(
            "Remember keys & today's token (.cache)",
            key="_w_remember_local",
            help="Saves API keys & today's token to gitignored .cache/broker_tokens.json across browser tabs and reloads. Leave unchecked on public cloud apps.",
        )
    )
    st.session_state._local_persist_enabled = checked
    return checked


def _render_token_input_with_clear(
    label: str,
    state_key: str,
    widget_key: str,
    placeholder: str,
    help_text: str,
    clear_callback: Any,
    clear_key: str,
    clear_help: str,
) -> str:
    """Render password token box alongside a compact clear action button."""
    if widget_key not in st.session_state:
        st.session_state[widget_key] = st.session_state.get(state_key, "")
    has_token = bool(str(st.session_state.get(widget_key, "") or "").strip())
    col_tok, col_clr = st.columns([4.2, 1.0], vertical_alignment="bottom")
    with col_tok:
        val = st.text_input(
            label,
            type="password",
            key=widget_key,
            placeholder=placeholder,
            help=help_text,
        ).strip()
    with col_clr:
        st.button(
            "✕",
            key=clear_key,
            help=clear_help,
            disabled=not (has_token or bool(val)),
            on_click=clear_callback,
            width="stretch",
        )
    st.session_state[state_key] = val
    return val


def _render_redirect_uri_with_apply(
    label: str,
    state_key: str,
    widget_key: str,
    apply_key: str,
) -> str:
    """Render Redirect/Response URI input paired with a commit button so link URLs update immediately."""
    if widget_key not in st.session_state:
        st.session_state[widget_key] = st.session_state.get(state_key, "")
    col_url, col_apply = st.columns([4.2, 1.0], vertical_alignment="bottom")
    with col_url:
        val = st.text_input(
            label,
            key=widget_key,
            help="Press Enter or click ✓ after editing to refresh the login link below.",
        ).strip()
    with col_apply:
        st.button(
            "✓",
            key=apply_key,
            help="Apply edited Redirect URI and refresh login link",
            width="stretch",
        )
    st.session_state[state_key] = val
    return val


def _render_upstox_sidebar_auth(wss_window_open: bool, detected_redirect_uri: str) -> tuple[str, bool]:
    """Render Upstox v3 token input, WSS toggle, and OAuth daily login helper in sidebar."""
    upstox_token = _render_token_input_with_clear(
        label="Upstox Access Token (Daily)",
        state_key="upstox_access_token",
        widget_key="_w_upstox_token",
        placeholder="Paste today's Upstox access_token",
        help_text="Upstox tokens expire daily. Paste today's token or use the OAuth Login Helper below.",
        clear_callback=_clear_upstox_token,
        clear_key="clear_upstox_token_btn",
        clear_help="Clear saved Upstox token",
    )
    if upstox_token:
        st.session_state._upstox_cleared_explicitly = False
    upstox_notice = st.session_state.pop("_upstox_login_notice", None)
    if upstox_notice:
        st.success(upstox_notice)
    elif upstox_token:
        st.caption("✅ Access token loaded for today's session")
    else:
        st.caption("⚠️ Access token required — paste above or sign in below")

    upstox_live_ws = st.checkbox(
        "Stream Live WSS (MarketDataStreamerV3)",
        value=True,
        help="Stream live full-quote ticks over WebSocket (zero REST polling once initialized).",
    )
    if not upstox_live_ws and upstox_token:
        stop_upstox_stream(upstox_token)
    elif upstox_live_ws:
        st.caption(
            "🟢 WSS active window (09:05–15:35 IST)"
            if wss_window_open
            else "⏸️ Market closed — auto-uses 1-call Batch Quote / cache"
        )

    with st.expander("🔑 Daily Login Helper (OAuth Code)", expanded=not bool(upstox_token)):
        creds_ready = all(
            bool(st.session_state.get(k, "").strip())
            for k in ("_u_client_id", "_u_client_secret", "_u_redirect_uri")
        )
        edit_u_creds = not creds_ready or st.checkbox(
            "Edit API credentials",
            value=False,
            key="_w_edit_upstox_creds",
            help="API Key, Secret, and Redirect URI are loaded — check to view or modify them.",
        )
        if edit_u_creds:
            u_cid = _sync_text_field("API Key (client_id)", "_u_client_id", "_w_u_client_id")
            u_csec = _sync_text_field(
                "API Secret", "_u_client_secret", "_w_u_client_secret", type="password"
            )
            u_redir = _render_redirect_uri_with_apply(
                "Redirect URI", "_u_redirect_uri", "_w_u_redirect_uri", "_apply_u_redir_btn"
            )
        else:
            u_cid = st.session_state.get("_u_client_id", "").strip()
            u_csec = st.session_state.get("_u_client_secret", "").strip()
            u_redir = st.session_state.get("_u_redirect_uri", detected_redirect_uri).strip()
            st.caption("✅ API Key, Secret & Redirect URI loaded")

        remember_local = _render_local_persist_toggle()
        if remember_local:
            _save_local_if_changed(
                "upstox",
                {
                    "access_token": upstox_token,
                    "client_id": u_cid,
                    "client_secret": u_csec,
                    "redirect_uri": u_redir,
                },
            )

        if u_cid and u_redir:
            u_state = _ensure_oauth_link_state(
                {"client_id": u_cid, "client_secret": u_csec, "redirect_uri": u_redir},
                "_u_oauth_nonce",
                "_u_oauth_snap",
            )
            login_url = build_upstox_login_url(u_cid, u_redir, state=u_state)
            st.caption(f"Callback URL: `{u_redir}`")
            _render_same_tab_login_link(
                "1️⃣ Open Upstox Daily Login", login_url, "upstox_same_tab_login"
            )
        else:
            st.caption("Fill API Key and Redirect URI to generate login link.")

        u_code_raw = _sync_text_field(
            "2️⃣ Auth Code or Full Redirect URL",
            "_u_auth_code",
            "_w_u_auth_code",
            placeholder="Paste code or full ?code=... callback URL",
            help="Accepts either the bare authorization code or the full callback URL after login.",
        )
        auto_up_exchange = bool(st.session_state.pop("_auto_exchange_upstox", False))
        if auto_up_exchange and not (u_code_raw and u_cid and u_csec and u_redir):
            st.info("Auth code captured from redirect URL — enter API credentials and click **Exchange Code for Token**.")

        if st.button("Exchange Code for Token", width="stretch") or (
            auto_up_exchange and u_code_raw and u_cid and u_csec and u_redir
        ):
            u_code = extract_oauth_param(u_code_raw, ("code",))
            if not (u_code and u_cid and u_csec and u_redir):
                st.warning("Fill API Key, API Secret, Redirect URI, and Auth Code.")
            else:
                with st.spinner("Exchanging Upstox authorization code..."):
                    tok, err = exchange_upstox_auth_code(u_code, u_cid, u_csec, u_redir)
                if tok:
                    active_state = st.session_state.pop("_active_up_oauth_state", "")
                    if active_state:
                        store_completed_oauth_token(active_state, {"access_token": tok})
                    if remember_local:
                        save_local_broker_auth(
                            "upstox",
                            {
                                "access_token": tok,
                                "client_id": u_cid,
                                "client_secret": u_csec,
                                "redirect_uri": u_redir,
                            },
                        )
                    st.session_state._pending_upstox_token = tok
                    st.session_state._upstox_login_notice = "Upstox token saved for today's session — ready to scan."
                    st.rerun()
                else:
                    st.error(err or "Upstox token exchange failed.")

    return upstox_token, upstox_live_ws


def _render_fivepaisa_sidebar_auth(wss_window_open: bool) -> tuple[str, str, str, bool]:
    """Render 5paisa Xstream JWT input, WSS toggle, and TOTP/OAuth daily login helper in sidebar."""
    fivepaisa_token = _render_token_input_with_clear(
        label="5paisa Access Token (Daily)",
        state_key="fivepaisa_access_token",
        widget_key="_w_fivepaisa_token",
        placeholder="Paste today's JWT access_token or use TOTP Login below",
        help_text="5paisa JWT access tokens expire daily. Generate one via TOTP/PIN or OAuth below.",
        clear_callback=_clear_fivepaisa_token,
        clear_key="clear_fivepaisa_token_btn",
        clear_help="Clear saved 5paisa token",
    )
    if fivepaisa_token:
        st.session_state._fivepaisa_cleared_explicitly = False
    jwt_info = inspect_fivepaisa_jwt(fivepaisa_token) if fivepaisa_token else {
        "is_jwt": False,
        "client_code": "",
        "exp_ts": None,
        "is_expired": False,
        "exp_label": "",
    }
    if jwt_info["client_code"] and not st.session_state.get("fivepaisa_client_code", "").strip():
        st.session_state.fivepaisa_client_code = jwt_info["client_code"]
        st.session_state._w_fivepaisa_client_code = jwt_info["client_code"]

    fivepaisa_client_code = _sync_text_field(
        "5paisa Client Code",
        "fivepaisa_client_code",
        "_w_fivepaisa_client_code",
        placeholder="e.g. 50011110 (auto-read from JWT)",
        help="Required for MarketFeedV3 WebSocket subscriptions (auto-inferred from JWT if omitted).",
    )
    fivepaisa_notice = st.session_state.pop("_fivepaisa_login_notice", None)
    if fivepaisa_notice:
        st.success(fivepaisa_notice)
    elif fivepaisa_token and jwt_info["is_expired"]:
        st.warning(f"⚠️ 5paisa JWT expired ({jwt_info['exp_label']}) — generate a fresh token below.")
    elif fivepaisa_token and jwt_info["exp_label"]:
        st.caption(f"✅ JWT loaded · valid until {jwt_info['exp_label']}")
    elif fivepaisa_token:
        st.caption("✅ JWT token loaded for today's session")
    else:
        st.caption("⚠️ JWT token required — paste above or sign in below")

    fivepaisa_live_ws = st.checkbox(
        "Stream Live WSS (MarketFeedV3)",
        value=True,
        help="Stream live NSE MarketFeedV3 ticks over WebSocket (up to 3,000 scrips, 50/batch).",
    )
    if not fivepaisa_live_ws and fivepaisa_token:
        stop_fivepaisa_stream(fivepaisa_token, fivepaisa_client_code)
    elif fivepaisa_live_ws:
        st.caption(
            "🟢 WSS active window (09:05–15:35 IST)"
            if wss_window_open
            else "⏸️ Market closed — auto-uses cached / REST seed"
        )

    helper_expanded = (not bool(fivepaisa_token)) or bool(jwt_info["is_expired"])
    with st.expander("🔑 Daily Login Helper (TOTP / OAuth)", expanded=helper_expanded):
        fp_keys_ready = all(
            bool(st.session_state.get(k, "").strip())
            for k in ("_fp_user_key", "_fp_enc_key", "_fp_user_id")
        )
        edit_fp_creds = not fp_keys_ready or st.checkbox(
            "Edit API keys",
            value=False,
            key="_w_edit_fp_creds",
            help="API Key, Encryption Key, and User ID are loaded — check to view or modify them.",
        )
        if edit_fp_creds:
            st.caption("From **xstream.5paisa.com → My App** (`App Source` & `User Password` are not needed).")
            fp_user_key = _sync_text_field(
                "API Key (User Key / VendorKey)",
                "_fp_user_key",
                "_w_fp_user_key",
                placeholder="API Key from Xstream My App card",
                help="Labeled 'API Key' on the 5paisa Xstream My App card (called User Key / VendorKey in API docs).",
            )
            fp_enc_key = _sync_text_field(
                "Encryption Key",
                "_fp_enc_key",
                "_w_fp_enc_key",
                type="password",
                placeholder="Encryption Key from Xstream dashboard",
            )
            fp_user_id = _sync_text_field(
                "User ID",
                "_fp_user_id",
                "_w_fp_user_id",
                placeholder="User ID from Xstream dashboard",
            )
        else:
            fp_user_key = st.session_state.get("_fp_user_key", "").strip()
            fp_enc_key = st.session_state.get("_fp_enc_key", "").strip()
            fp_user_id = st.session_state.get("_fp_user_id", "").strip()
            st.caption("✅ API Key, Encryption Key & User ID loaded")

        remember_local = _render_local_persist_toggle()
        if remember_local:
            _save_local_if_changed(
                "fivepaisa",
                {
                    "access_token": fivepaisa_token,
                    "client_code": fivepaisa_client_code,
                    "user_key": fp_user_key,
                    "encryption_key": fp_enc_key,
                    "user_id": fp_user_id,
                },
            )

        tab_totp, tab_oauth = st.tabs(["TOTP + PIN", "OAuth Redirect"])
        with tab_totp:
            if "_w_fp_totp_cc" not in st.session_state:
                st.session_state._w_fp_totp_cc = (
                    fivepaisa_client_code or _get_broker_secret("fivepaisa", "client_code")
                )
            fp_cc = st.text_input(
                "Client Code",
                key="_w_fp_totp_cc",
                placeholder="e.g. 50764390 (your 5paisa login ID)",
                help="Your numeric 5paisa trading Client Code (without any '5P' app-name prefix).",
            ).strip()
            fp_pin = _sync_text_field("Trading PIN", "_fp_totp_pin", "_w_fp_totp_pin", type="password")
            fp_totp_val = _sync_text_field(
                "TOTP (6-digit code or Base32 Secret)",
                "_fp_totp_secret",
                "_w_fp_totp_secret",
                type="password",
            )
            if fp_cc and fp_pin and fp_totp_val and fp_keys_ready:
                st.caption("✅ TOTP/PIN ready — click below to generate today's token")
            if st.button("Login via TOTP & Save Token", width="stretch"):
                if not (fp_cc and fp_pin and fp_totp_val and fp_user_key and fp_enc_key and fp_user_id):
                    st.warning("Fill API Key, Encryption Key, User ID, Client Code, PIN, and TOTP.")
                else:
                    with st.spinner("Logging in to 5paisa via TOTP..."):
                        tok, ccode, err = login_fivepaisa_totp(
                            fp_cc, fp_totp_val, fp_pin, fp_user_key, fp_enc_key, fp_user_id
                        )
                    if tok:
                        resolved_cc = ccode or fp_cc
                        if remember_local:
                            save_local_broker_auth(
                                "fivepaisa",
                                {
                                    "access_token": tok,
                                    "client_code": resolved_cc,
                                    "user_key": fp_user_key,
                                    "encryption_key": fp_enc_key,
                                    "user_id": fp_user_id,
                                },
                            )
                        st.session_state._pending_fivepaisa_auth = (tok, resolved_cc)
                        st.session_state._fivepaisa_login_notice = (
                            "5paisa token generated via TOTP for today's session."
                        )
                        st.rerun()
                    else:
                        st.error(err or "5paisa TOTP login failed.")
        with tab_oauth:
            fp_redir = _render_redirect_uri_with_apply(
                "Response URL", "_fp_redirect_uri", "_w_fp_redirect_uri", "_apply_fp_redir_btn"
            )
            if remember_local:
                _save_local_if_changed(
                    "fivepaisa",
                    {
                        "access_token": fivepaisa_token,
                        "client_code": fivepaisa_client_code,
                        "user_key": fp_user_key,
                        "encryption_key": fp_enc_key,
                        "user_id": fp_user_id,
                        "redirect_uri": fp_redir,
                    },
                )
            if fp_user_key and fp_redir:
                fp_state = _ensure_oauth_link_state(
                    {
                        "user_key": fp_user_key,
                        "encryption_key": fp_enc_key,
                        "user_id": fp_user_id,
                        "redirect_uri": fp_redir,
                    },
                    "_fp_oauth_nonce",
                    "_fp_oauth_snap",
                )
                fp_login_url = build_fivepaisa_login_url(fp_user_key, fp_redir, state=fp_state)
                st.caption(f"Callback URL: `{fp_redir}`")
                _render_same_tab_login_link(
                    "1️⃣ Open 5paisa Daily Login", fp_login_url, "fivepaisa_same_tab_login"
                )
            else:
                st.caption("Fill API Key and Response URL to generate login link.")

            fp_req_raw = _sync_text_field(
                "2️⃣ RequestToken or Full Redirect URL",
                "_fp_req_token",
                "_w_fp_req_token",
                placeholder="Paste token or full ?RequestToken=... callback URL",
                help="Accepts either the bare RequestToken or the full callback URL after login.",
            )
            auto_fp_exchange = bool(st.session_state.pop("_auto_exchange_fivepaisa", False))
            if auto_fp_exchange and not (fp_req_raw and fp_user_key and fp_enc_key and fp_user_id):
                st.info("RequestToken captured from redirect URL — enter API Key, Encryption Key, and User ID, then click **Exchange RequestToken**.")

            if st.button("Exchange RequestToken", width="stretch") or (
                auto_fp_exchange and fp_req_raw and fp_user_key and fp_enc_key and fp_user_id
            ):
                fp_req_tok = extract_oauth_param(
                    fp_req_raw, ("RequestToken", "requesttoken", "request_token")
                )
                if not (fp_req_tok and fp_user_key and fp_enc_key and fp_user_id):
                    st.warning("Fill API Key, Encryption Key, User ID, and RequestToken.")
                else:
                    with st.spinner("Exchanging 5paisa RequestToken..."):
                        tok, ccode, err = exchange_fivepaisa_request_token(
                            fp_req_tok, fp_user_key, fp_enc_key, fp_user_id
                        )
                    if tok:
                        resolved_cc = ccode or fivepaisa_client_code
                        active_state = st.session_state.pop("_active_fp_oauth_state", "")
                        if active_state:
                            store_completed_oauth_token(
                                active_state,
                                {"access_token": tok, "client_code": resolved_cc},
                            )
                        if remember_local:
                            save_local_broker_auth(
                                "fivepaisa",
                                {
                                    "access_token": tok,
                                    "client_code": resolved_cc,
                                    "user_key": fp_user_key,
                                    "encryption_key": fp_enc_key,
                                    "user_id": fp_user_id,
                                    "redirect_uri": fp_redir,
                                },
                            )
                        st.session_state._pending_fivepaisa_auth = (tok, ccode or "")
                        st.session_state._fivepaisa_login_notice = "5paisa token saved for today's session — ready to scan."
                        st.rerun()
                    else:
                        st.error(err or "5paisa RequestToken exchange failed.")

    return fivepaisa_token, fivepaisa_client_code, fp_user_key, fivepaisa_live_ws


_apply_pending_auth_updates()
_capture_oauth_redirect_params()

# Sidebar controls
with st.sidebar:
    st.caption("**1. Data Source & Connection**")
    data_source_raw = st.segmented_control(
        "Data Source",
        options=["Yahoo Finance", "Upstox v3", "5paisa Xstream"],
        selection_mode="single",
        key="data_source_choice",
        help="Use free Yahoo Finance (no login), Upstox v3, or 5paisa Xstream (daily token + real-time WebSocket stream).",
    )
    data_source = data_source_raw if data_source_raw in ("Upstox v3", "5paisa Xstream") else "Yahoo Finance"
    use_upstox = data_source == "Upstox v3"
    use_fivepaisa = data_source == "5paisa Xstream"
    wss_window_open = is_wss_active_window(background_refresh=True)

    oauth_error_notice = st.session_state.pop("_oauth_error_notice", None)
    if oauth_error_notice:
        st.error(oauth_error_notice)

    upstox_token = ""
    upstox_live_ws = True
    fivepaisa_token = ""
    fivepaisa_client_code = ""
    fivepaisa_user_key = ""
    fivepaisa_live_ws = True
    if use_upstox:
        upstox_token, upstox_live_ws = _render_upstox_sidebar_auth(
            wss_window_open, _detected_redirect_uri
        )
    elif use_fivepaisa:
        fivepaisa_token, fivepaisa_client_code, fivepaisa_user_key, fivepaisa_live_ws = _render_fivepaisa_sidebar_auth(
            wss_window_open
        )

    st.divider()
    st.caption("**2. Volume, Gap & Interval**")
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
    max_workers = 8
    if data_source == "Yahoo Finance":
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

    st.divider()
    st.caption("**3. OI, Sorting & Momentum**")
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

    st.divider()
    st.caption("**4. Auto-Refresh & Universe**")
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
    st.divider()
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
    fivepaisa_user_key: str = "",
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

    col1, col2, col3, col_btn = st.columns([1.4, 1.05, 0.95, 1.05], vertical_alignment="bottom")
    col1.metric("Current IST", now.strftime("%Y-%m-%d %H:%M:%S"))
    prev_day_slot = col2.empty()
    col3.metric("Market Status", "🟢 OPEN" if market_open else "🔴 CLOSED")
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
                        user_key=fivepaisa_user_key,
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
            wss_off_hours = not is_wss_active_window(now_after, background_refresh=True)
            if wss_off_hours:
                enforce_market_hours_shutdown()
            broker_ws_selected = (
                (data_source == "Upstox v3" and upstox_live_ws)
                or (data_source == "5paisa Xstream" and fivepaisa_live_ws)
            )
            if broker_ws_selected and wss_off_hours:
                info_slot.caption(
                    "⏸️ **Auto-refresh & Live WSS paused** (market closed). Manual **Scan Now** still works via REST/Batch Quote."
                )
            else:
                info_slot.caption("⏸️ **Auto-refresh paused** (market closed). Manual **Scan Now** still works anytime.")

    # Client-side filtering & sorting on the cached scan snapshot for the active source & interval
    raw_df = st.session_state.results_by_interval.get(snapshot_key, pd.DataFrame())
    raw_empty_dt = st.session_state._last_scan_empty_by_interval.get(snapshot_key)
    empty_scan_dt = raw_empty_dt if (raw_empty_dt is not None and raw_empty_dt.date() == now.date()) else None
    filter_cache_key = (
        snapshot_key,
        id(raw_df),
        last_scan_dt,
        float(min_ratio),
        int(min_volume),
        bool(exclude_gap_up),
        bool(exclude_gap_down),
        float(gap_threshold),
        bool(include_oi),
        None if min_oi_chg_pct is None else float(min_oi_chg_pct),
        tuple(oi_buildup_filter),
        sort_by,
        momentum_filter_state,
    )
    cached_filter = st.session_state.get("_filtered_scan_cache")
    if cached_filter is not None and cached_filter[0] == filter_cache_key:
        filter_result = cached_filter[1]
    else:
        filter_result = _filter_and_sort_results(
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
        st.session_state._filtered_scan_cache = (filter_cache_key, filter_result)
    df, gap_missing, gap_excluded_count, oi_missing, oi_excluded_count, momentum_missing, momentum_excluded_count = filter_result

    has_signals = include_oi and (not df.empty) and ("Signal" in df.columns)
    table_df = df
    sig_choice = "All"

    with results_slot:
        if df.empty:
            if last_scan_dt is None and empty_scan_dt is None:
                with st.container(border=True):
                    st.markdown(f"**Ready to scan {len(symbols)} NSE F&O stocks**")
                    oi_label = "On" if include_oi else "Off"
                    st.caption(
                        f"Source: **{data_source}** · Interval: **{interval}** · "
                        f"Min Ratio: **{min_ratio:.1f}×** · Min Volume: **{min_volume:,}** · NSE OI: **{oi_label}**"
                    )
                    st.caption(
                        "Click **🔄 Scan Now** above (or enable **Auto-refresh** in the sidebar). "
                        f"Highlights: {SIGNAL_BULLISH} (Ratio > 1.0×, OI Chg > +10%, % Chg > +1.5%) · "
                        f"{SIGNAL_BEARISH} (Ratio > 1.0×, OI Chg > +10%, % Chg < -1.5%)."
                    )
            elif not raw_df.empty:
                st.warning("No stocks matched the current filters. Try lowering the Minimum Ratio or Volume.")
        else:
            st.session_state.selected_columns = [
                c for c in st.session_state.selected_columns if c in ALL_COLUMNS
            ]
            if has_signals:
                bullish_count = int((df["Signal"] == SIGNAL_BULLISH).sum())
                bearish_count = int((df["Signal"] == SIGNAL_BEARISH).sum())
                total_count = len(df)
                col_sig_summary, col_sig_pills, col_cols_btn, col_csv_btn = st.columns(
                    [2.5, 2.0, 1.0, 0.85], vertical_alignment="center"
                )
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
            else:
                col_plain_summary, col_cols_btn, col_csv_btn = st.columns(
                    [4.5, 1.0, 0.85], vertical_alignment="center"
                )
                with col_plain_summary:
                    st.caption(f"Showing **{len(table_df)}** stocks · NSE OI disabled")

            if not table_df.empty and "Chart" in st.session_state.selected_columns and "Symbol" in table_df.columns:
                table_df = table_df.assign(
                    Chart="https://in.tradingview.com/chart/?symbol=NSE%3A" + table_df["Symbol"].astype(str)
                )

            with col_cols_btn:
                with st.popover("⚙️ Columns", width="stretch", help="Add, remove, or reorder visible columns."):
                    st.multiselect(
                        "Visible columns (add, remove, or reorder)",
                        options=ALL_COLUMNS,
                        format_func=lambda col: COLUMN_LABELS.get(col, col),
                        key="selected_columns",
                        on_change=_mark_ui_interaction,
                    )
                    st.button("Reset to Default", on_click=_reset_default_columns)

            visible_cols = [c for c in st.session_state.selected_columns if c in table_df.columns]
            csv_data = b""
            csv_ts = (last_scan_dt or now).strftime("%Y%m%d_%H%M")
            if not table_df.empty and visible_cols:
                display_df = table_df[visible_cols]
                csv_cache_key = (filter_cache_key, sig_choice, tuple(visible_cols))
                cached_csv = st.session_state.get("_csv_export_cache")
                if cached_csv is not None and cached_csv[0] == csv_cache_key:
                    csv_data = cached_csv[1]
                else:
                    csv_data = display_df.to_csv(index=False).encode("utf-8")
                    st.session_state._csv_export_cache = (csv_cache_key, csv_data)
            with col_csv_btn:
                st.download_button(
                    "⬇️ CSV",
                    data=csv_data,
                    file_name=f"relative_volume_{interval}_{csv_ts}.csv",
                    mime="text/csv",
                    key=f"download_csv_{interval}",
                    width="stretch",
                    disabled=(table_df.empty or not visible_cols),
                    on_click="ignore",
                )

            if table_df.empty:
                st.info(f"No **{sig_choice}** stocks matched the current scan and filters.")
            elif not visible_cols:
                st.warning("Select at least one column in **⚙️ Columns** to display results.")
            else:
                active_config = {c: COLUMN_CONFIG[c] for c in visible_cols if c in COLUMN_CONFIG}
                table_data = (
                    _style_scanner_df(display_df, table_df["Signal"])
                    if has_signals
                    else display_df
                )
                table_height = min(620, max(240, 40 + 35 * len(display_df)))

                st.dataframe(
                    table_data,
                    width="stretch",
                    height=table_height,
                    hide_index=True,
                    column_config=active_config,
                    placeholder="—",
                    key="scanner_table",
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
                elif mode == "post_close_cached":
                    prov_note = f" | {p_name} Post-Close Cached"
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
    fivepaisa_user_key=fivepaisa_user_key,
    fivepaisa_live_ws=fivepaisa_live_ws,
)

st.divider()
with st.expander("📘 Column & Momentum Guide — How to Use", expanded=False):
    st.markdown(COLUMN_GUIDE_MARKDOWN)
st.caption(
    "Data sources: Yahoo Finance, Upstox v3, or 5paisa Xstream (intraday OHLCV / live WSS) & NSE India (F&O OI & holidays). "
    "For educational / research purposes only — not investment advice."
)
