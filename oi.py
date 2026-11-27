"""
NSE F&O Open Interest (OI) & Day Change in OI helper.

Uses NSE India's aggregate OI spurts endpoint (1 request covers all F&O underlyings)
with strict rate-limit safeguards:
- Single-flight locking & minimum fetch interval (>=45s live, >=5m when market is closed)
- Persistent curl_cffi browser-impersonated session with automatic Akamai cookie refresh
- Exponential backoff cooldown (60s -> 600s) on HTTP 403/429/5xx or challenge pages
"""

from datetime import datetime, date
import math
import random
import threading
import time as pytime
from typing import Any, Dict, Optional, Tuple

from holidays import is_market_open
from utils import (
    IST,
    _orjson,
    decode_json_response,
    parse_nse_date as _parse_nse_date,
    parse_nse_ts as _parse_nse_ts,
)

_NSE_OI_WARMUP_URL = "https://www.nseindia.com/market-data/oi-spurts"
_NSE_OI_API_URL = "https://www.nseindia.com/api/live-analysis-oi-spurts-underlyings"

_MIN_FETCH_INTERVAL_SECONDS = 45.0
_CLOSED_CACHE_TTL_SECONDS = 300.0
_SESSION_MAX_AGE_SECONDS = 1800.0
_BASE_BACKOFF_SECONDS = 60.0
_MAX_BACKOFF_SECONDS = 600.0

_STATE_LOCK = threading.Lock()
_FETCH_LOCK = threading.Lock()

_OI_SESSION: Any = None
_OI_SESSION_CREATED_MONO: float = 0.0
_LAST_FETCH_MONO: float = 0.0
_COOLDOWN_UNTIL_MONO: float = 0.0
_CONSECUTIVE_FAILURES: int = 0
_LAST_ERROR: Optional[str] = None

# Latest cached snapshot: (trading_date, nse_ts_str, {symbol: {"oi", "oi_prev", "oi_chg", "oi_chg_pct"}})
_CACHED_SNAPSHOT: Optional[Tuple[date, str, Dict[str, Dict[str, Optional[float]]]]] = None


def _to_float(val: Any) -> Optional[float]:
    if val is None or val == "":
        return None
    try:
        num = float(val)
        return num
    except (TypeError, ValueError):
        return None


def _decode_json_response(resp: Any) -> Any:
    return decode_json_response(resp, orjson_mod=_orjson)


def _close_session_locked() -> None:
    global _OI_SESSION, _OI_SESSION_CREATED_MONO
    if _OI_SESSION is not None:
        try:
            _OI_SESSION.close()
        except Exception:
            pass
        _OI_SESSION = None
        _OI_SESSION_CREATED_MONO = 0.0


def _create_warmed_session(timeout: int = 6) -> Any:
    from curl_cffi import requests as curl_requests

    sess = curl_requests.Session(impersonate="chrome")
    try:
        sess.get(
            _NSE_OI_WARMUP_URL,
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=timeout,
        )
        return sess
    except Exception:
        try:
            sess.close()
        except Exception:
            pass
        raise


def _parse_oi_payload(
    payload: Any,
) -> Tuple[Optional[date], float, str, Dict[str, Dict[str, Optional[float]]]]:
    if not isinstance(payload, dict):
        raise ValueError("Unexpected NSE OI payload type")
    items = payload.get("data")
    if not isinstance(items, list) or not items:
        raise ValueError("Empty NSE OI data list")

    nse_ts_str = str(payload.get("timestamp") or "").strip()
    nse_dt = _parse_nse_ts(nse_ts_str) or datetime.now(IST)
    if not nse_ts_str:
        nse_ts_str = nse_dt.strftime("%d-%b-%Y %H:%M:%S")
    trading_date = _parse_nse_date(payload.get("currTradingDate")) or nse_dt.date()

    rows: Dict[str, Dict[str, Optional[float]]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        sym = str(item.get("symbol") or "").strip()
        if not sym:
            continue
        oi = _to_float(item.get("latestOI"))
        oi_prev = _to_float(item.get("prevOI"))
        oi_chg = _to_float(item.get("changeInOI"))
        if oi_chg is None and oi is not None and oi_prev is not None:
            oi_chg = oi - oi_prev
        oi_chg_pct = _to_float(item.get("avgInOI"))
        if oi_chg_pct is None and oi_chg is not None and oi_prev is not None and oi_prev > 0:
            oi_chg_pct = (oi_chg / oi_prev) * 100.0
        rows[sym] = {
            "oi": oi,
            "oi_prev": oi_prev,
            "oi_chg": oi_chg,
            "oi_chg_pct": oi_chg_pct,
        }

    if not rows:
        raise ValueError("No valid symbol rows in NSE OI payload")
    return trading_date, nse_dt.timestamp(), nse_ts_str, rows


def _build_snapshot_result_locked(
    now_mono: float,
    from_cache: bool,
    error_msg: Optional[str] = None,
) -> Dict[str, Any]:
    cooldown_s = max(0.0, _COOLDOWN_UNTIL_MONO - now_mono)
    if _CACHED_SNAPSHOT is None:
        return {
            "by_symbol": {},
            "nse_timestamp": None,
            "from_cache": from_cache,
            "cooldown_seconds": cooldown_s,
            "error": error_msg,
        }

    _, cur_ts_str, cur_rows = _CACHED_SNAPSHOT
    return {
        "by_symbol": cur_rows,
        "nse_timestamp": cur_ts_str,
        "from_cache": from_cache,
        "cooldown_seconds": cooldown_s,
        "error": error_msg,
    }


def _fetch_nse_oi_raw(timeout: int = 6) -> Any:
    global _OI_SESSION, _OI_SESSION_CREATED_MONO
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": _NSE_OI_WARMUP_URL,
        "X-Requested-With": "XMLHttpRequest",
    }

    now_mono = pytime.monotonic()
    if _OI_SESSION is None or (now_mono - _OI_SESSION_CREATED_MONO) > _SESSION_MAX_AGE_SECONDS:
        _close_session_locked()
        _OI_SESSION = _create_warmed_session(timeout=timeout)
        _OI_SESSION_CREATED_MONO = pytime.monotonic()

    resp = _OI_SESSION.get(_NSE_OI_API_URL, headers=headers, timeout=timeout)
    if resp.status_code in (401, 403):
        # Single jittered re-warm attempt on expired Akamai cookie
        _close_session_locked()
        pytime.sleep(0.8 + random.uniform(0.0, 0.4))
        _OI_SESSION = _create_warmed_session(timeout=timeout)
        _OI_SESSION_CREATED_MONO = pytime.monotonic()
        resp = _OI_SESSION.get(_NSE_OI_API_URL, headers=headers, timeout=timeout)

    if resp.status_code == 429:
        raise RuntimeError("NSE OI rate limited (HTTP 429)")
    if resp.status_code != 200:
        raise RuntimeError(f"NSE OI HTTP {resp.status_code}")

    return _decode_json_response(resp)


def get_oi_snapshot(
    force_refresh: bool = False,
    min_interval_seconds: float = _MIN_FETCH_INTERVAL_SECONDS,
    timeout: int = 6,
) -> Dict[str, Any]:
    """
    Fetch latest NSE F&O OI & Day Change in OI with strict rate-limit protection.
    Returns dict with keys:
      by_symbol, nse_timestamp, from_cache, cooldown_seconds, error.
    """
    global _LAST_FETCH_MONO, _COOLDOWN_UNTIL_MONO, _CONSECUTIVE_FAILURES, _LAST_ERROR, _CACHED_SNAPSHOT

    now_ist = datetime.now(IST)
    market_open = is_market_open(now_ist)
    effective_min_interval = max(float(min_interval_seconds), _MIN_FETCH_INTERVAL_SECONDS)

    def _can_serve_cached(now_mono: float) -> Optional[Dict[str, Any]]:
        if now_mono < _COOLDOWN_UNTIL_MONO:
            return _build_snapshot_result_locked(now_mono, from_cache=True, error_msg=_LAST_ERROR)
        if _CACHED_SNAPSHOT is not None and _LAST_FETCH_MONO > 0.0:
            age = now_mono - _LAST_FETCH_MONO
            if not force_refresh and age < effective_min_interval:
                return _build_snapshot_result_locked(now_mono, from_cache=True)
            if not market_open and age < _CLOSED_CACHE_TTL_SECONDS:
                return _build_snapshot_result_locked(now_mono, from_cache=True)
        return None

    with _STATE_LOCK:
        cached = _can_serve_cached(pytime.monotonic())
        if cached is not None:
            return cached

    with _FETCH_LOCK:
        with _STATE_LOCK:
            cached = _can_serve_cached(pytime.monotonic())
            if cached is not None:
                return cached

        try:
            payload = _fetch_nse_oi_raw(timeout=timeout)
            trading_date, _, nse_ts_str, rows = _parse_oi_payload(payload)
        except Exception as exc:
            with _STATE_LOCK:
                _close_session_locked()
                _CONSECUTIVE_FAILURES += 1
                backoff = min(
                    _MAX_BACKOFF_SECONDS,
                    _BASE_BACKOFF_SECONDS * (2 ** (_CONSECUTIVE_FAILURES - 1)),
                )
                now_mono = pytime.monotonic()
                _COOLDOWN_UNTIL_MONO = now_mono + backoff
                _LAST_ERROR = str(exc)
                return _build_snapshot_result_locked(now_mono, from_cache=True, error_msg=_LAST_ERROR)

        with _STATE_LOCK:
            _CONSECUTIVE_FAILURES = 0
            _COOLDOWN_UNTIL_MONO = 0.0
            _LAST_ERROR = None
            _LAST_FETCH_MONO = pytime.monotonic()
            _CACHED_SNAPSHOT = (trading_date, nse_ts_str, rows)
            return _build_snapshot_result_locked(_LAST_FETCH_MONO, from_cache=False)


OI_BUILDUP_OPTIONS: Tuple[str, ...] = (
    "Long Buildup",
    "Short Buildup",
    "Short Covering",
    "Long Unwinding",
)

_OI_BUILDUP_QUADRANTS: Dict[Tuple[bool, bool], str] = {
    (True, True): "Long Buildup",
    (False, True): "Short Buildup",
    (True, False): "Short Covering",
    (False, False): "Long Unwinding",
}


def classify_oi_buildup(
    pct_change: Optional[float],
    oi_chg: Optional[float],
) -> Optional[str]:
    """Classify intraday price change vs day OI change into standard F&O buildup regimes."""
    if pct_change is None or oi_chg is None:
        return None
    try:
        p = float(pct_change)
        o = float(oi_chg)
    except (TypeError, ValueError):
        return None
    if math.isnan(p) or math.isnan(o):
        return None
    if p == 0.0 or o == 0.0:
        return "Neutral"
    return _OI_BUILDUP_QUADRANTS[(p > 0.0, o > 0.0)]


SIGNAL_BULLISH = "🟢 Bullish"
SIGNAL_BEARISH = "🔴 Bearish"
SIGNAL_NEUTRAL = "—"


def classify_trade_signal(
    ratio: Optional[float],
    oi_chg_pct: Optional[float],
    pct_change: Optional[float],
    min_ratio: float = 1.0,
    min_oi_chg_pct: float = 10.0,
    min_price_chg_pct: float = 1.5,
) -> str:
    """
    Classify Bullish / Bearish setup when all three criteria hold strictly:
    - Ratio > min_ratio (higher volume till now vs previous day till same time)
    - OI Chg % (Day) > min_oi_chg_pct (+10% fresh buildup)
    - Pct_Change > +min_price_chg_pct (Bullish) or < -min_price_chg_pct (Bearish)
    """
    if ratio is None or oi_chg_pct is None or pct_change is None:
        return SIGNAL_NEUTRAL
    try:
        r = float(ratio)
        o = float(oi_chg_pct)
        p = float(pct_change)
    except (TypeError, ValueError):
        return SIGNAL_NEUTRAL
    if math.isnan(r) or math.isnan(o) or math.isnan(p):
        return SIGNAL_NEUTRAL
    if r <= min_ratio or o <= min_oi_chg_pct:
        return SIGNAL_NEUTRAL
    if p > min_price_chg_pct:
        return SIGNAL_BULLISH
    if p < -min_price_chg_pct:
        return SIGNAL_BEARISH
    return SIGNAL_NEUTRAL
