"""
NSE F&O Open Interest (OI) & rolling 5-minute Change in OI helper.

Uses NSE India's aggregate OI spurts endpoint (1 request covers all F&O underlyings)
with strict rate-limit safeguards:
- Single-flight locking & minimum fetch interval (>=45s live, >=5m when market is closed)
- Persistent curl_cffi browser-impersonated session with automatic Akamai cookie refresh
- Exponential backoff cooldown (60s -> 600s) on HTTP 403/429/5xx or challenge pages
- Bounded 30-minute rolling snapshot buffer (persisted atomically in .cache/) for 5m ΔOI
"""

from collections import deque
from datetime import datetime, date
import json
import math
import os
from pathlib import Path
import random
import tempfile
import threading
import time as pytime
from typing import Any, Deque, Dict, List, Optional, Tuple

from holidays import IST, is_market_open

_NSE_OI_WARMUP_URL = "https://www.nseindia.com/market-data/oi-spurts"
_NSE_OI_API_URL = "https://www.nseindia.com/api/live-analysis-oi-spurts-underlyings"
_DISK_CACHE_FILE = Path(__file__).parent / ".cache" / "nse_oi_history.json"

_MIN_FETCH_INTERVAL_SECONDS = 45.0
_CLOSED_CACHE_TTL_SECONDS = 300.0
_SESSION_MAX_AGE_SECONDS = 1800.0
_BASE_BACKOFF_SECONDS = 60.0
_MAX_BACKOFF_SECONDS = 600.0

_WINDOW_TARGET_SECONDS = 300.0
_WINDOW_MIN_SECONDS = 180.0
_WINDOW_MAX_SECONDS = 540.0
_HISTORY_RETENTION_SECONDS = 1800.0

_STATE_LOCK = threading.Lock()
_FETCH_LOCK = threading.Lock()

_OI_SESSION: Any = None
_OI_SESSION_CREATED_MONO: float = 0.0
_LAST_FETCH_MONO: float = 0.0
_COOLDOWN_UNTIL_MONO: float = 0.0
_CONSECUTIVE_FAILURES: int = 0
_LAST_ERROR: Optional[str] = None

_HISTORY_DATE: Optional[date] = None
# Each entry: (snap_epoch_sec, nse_ts_str, {symbol: {"oi": float, "oi_prev": float, "oi_chg": float, "oi_chg_pct": float}})
_OI_HISTORY: Deque[Tuple[float, str, Dict[str, Dict[str, Optional[float]]]]] = deque()
_DISK_LOADED: bool = False


def _to_float(val: Any) -> Optional[float]:
    if val is None or val == "":
        return None
    try:
        num = float(val)
        return num
    except (TypeError, ValueError):
        return None


def _parse_nse_ts(raw: Optional[str]) -> Optional[datetime]:
    text = (raw or "").strip()
    if not text:
        return None
    for fmt in ("%d-%b-%Y %H:%M:%S", "%d-%B-%Y %H:%M:%S", "%d-%b-%Y %H:%M"):
        try:
            dt_naive = datetime.strptime(text, fmt)
            localize = getattr(IST, "localize", None)
            return localize(dt_naive) if localize is not None else dt_naive.replace(tzinfo=IST)
        except ValueError:
            continue
    return None


def _parse_nse_date(raw: Optional[str]) -> Optional[date]:
    text = (raw or "").strip()
    if not text:
        return None
    for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


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


def _load_disk_history_locked(target_date: date) -> None:
    global _DISK_LOADED, _HISTORY_DATE
    if _DISK_LOADED:
        return
    _DISK_LOADED = True
    if not _DISK_CACHE_FILE.exists():
        return
    try:
        payload = json.loads(_DISK_CACHE_FILE.read_text(encoding="utf-8"))
        cached_date = _parse_nse_date(payload.get("date"))
        if cached_date != target_date:
            return
        snaps = payload.get("snapshots") or []
        now_epoch = datetime.now(IST).timestamp()
        cutoff = now_epoch - _HISTORY_RETENTION_SECONDS
        loaded: List[Tuple[float, str, Dict[str, Dict[str, Optional[float]]]]] = []
        for item in snaps:
            ts = _to_float(item.get("ts"))
            ts_str = str(item.get("ts_str") or "")
            rows = item.get("rows")
            if ts is not None and ts >= cutoff and isinstance(rows, dict) and rows:
                loaded.append((ts, ts_str, rows))
        if loaded:
            loaded.sort(key=lambda x: x[0])
            _HISTORY_DATE = cached_date
            _OI_HISTORY.clear()
            _OI_HISTORY.extend(loaded)
    except Exception:
        pass


def _save_disk_history(
    hist_date: date,
    snapshots: List[Tuple[float, str, Dict[str, Dict[str, Optional[float]]]]],
) -> None:
    tmp_path: Optional[Path] = None
    try:
        _DISK_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "date": hist_date.isoformat(),
            "snapshots": [
                {"ts": ts, "ts_str": ts_str, "rows": rows}
                for ts, ts_str, rows in snapshots
            ],
        }
        fd, raw_tmp = tempfile.mkstemp(
            dir=str(_DISK_CACHE_FILE.parent),
            prefix=".tmp_oi_hist_",
            suffix=".json",
        )
        tmp_path = Path(raw_tmp)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f)
            f.flush()
        os.replace(tmp_path, _DISK_CACHE_FILE)
    except Exception:
        if tmp_path is not None:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass


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
    if not _OI_HISTORY:
        return {
            "by_symbol": {},
            "nse_timestamp": None,
            "baseline_timestamp": None,
            "window_seconds": None,
            "elapsed_since_oldest_seconds": 0.0,
            "from_cache": from_cache,
            "cooldown_seconds": cooldown_s,
            "error": error_msg,
        }

    cur_ts, cur_ts_str, cur_rows = _OI_HISTORY[-1]
    oldest_ts = _OI_HISTORY[0][0]
    elapsed_oldest = max(0.0, cur_ts - oldest_ts)

    best_base: Optional[Tuple[float, str, Dict[str, Dict[str, Optional[float]]]]] = None
    best_dist = float("inf")
    for hist_entry in _OI_HISTORY:
        age = cur_ts - hist_entry[0]
        if _WINDOW_MIN_SECONDS <= age <= _WINDOW_MAX_SECONDS:
            dist = abs(age - _WINDOW_TARGET_SECONDS)
            if dist < best_dist:
                best_dist = dist
                best_base = hist_entry

    base_rows = best_base[2] if best_base is not None else None
    base_ts_str = best_base[1] if best_base is not None else None
    window_seconds = (cur_ts - best_base[0]) if best_base is not None else None

    by_symbol: Dict[str, Dict[str, Optional[float]]] = {}
    for sym, cur_rec in cur_rows.items():
        oi_val = cur_rec.get("oi")
        oi_5m_chg: Optional[float] = None
        oi_5m_chg_pct: Optional[float] = None

        if base_rows is not None and oi_val is not None:
            base_rec = base_rows.get(sym)
            base_oi = base_rec.get("oi") if base_rec is not None else None
            if base_oi is not None:
                oi_5m_chg = oi_val - base_oi
                if base_oi > 0:
                    oi_5m_chg_pct = (oi_5m_chg / base_oi) * 100.0

        by_symbol[sym] = {
            "oi": oi_val,
            "oi_prev": cur_rec.get("oi_prev"),
            "oi_chg": cur_rec.get("oi_chg"),
            "oi_chg_pct": cur_rec.get("oi_chg_pct"),
            "oi_5m_chg": oi_5m_chg,
            "oi_5m_chg_pct": oi_5m_chg_pct,
        }

    return {
        "by_symbol": by_symbol,
        "nse_timestamp": cur_ts_str,
        "baseline_timestamp": base_ts_str,
        "window_seconds": window_seconds,
        "elapsed_since_oldest_seconds": elapsed_oldest,
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

    return resp.json()


def get_oi_snapshot(
    force_refresh: bool = False,
    min_interval_seconds: float = _MIN_FETCH_INTERVAL_SECONDS,
    timeout: int = 6,
) -> Dict[str, Any]:
    """
    Fetch latest NSE F&O OI & rolling 5-minute Change in OI with strict rate-limit protection.
    Returns dict with keys:
      by_symbol, nse_timestamp, baseline_timestamp, window_seconds,
      elapsed_since_oldest_seconds, from_cache, cooldown_seconds, error.
    """
    global _LAST_FETCH_MONO, _COOLDOWN_UNTIL_MONO, _CONSECUTIVE_FAILURES, _LAST_ERROR, _HISTORY_DATE

    now_ist = datetime.now(IST)
    today_ist = now_ist.date()
    market_open = is_market_open(now_ist)
    effective_min_interval = max(float(min_interval_seconds), _MIN_FETCH_INTERVAL_SECONDS)

    def _can_serve_cached(now_mono: float) -> Optional[Dict[str, Any]]:
        if now_mono < _COOLDOWN_UNTIL_MONO:
            return _build_snapshot_result_locked(now_mono, from_cache=True, error_msg=_LAST_ERROR)
        if _OI_HISTORY and _LAST_FETCH_MONO > 0.0:
            age = now_mono - _LAST_FETCH_MONO
            if not force_refresh and age < effective_min_interval:
                return _build_snapshot_result_locked(now_mono, from_cache=True)
            if not market_open and age < _CLOSED_CACHE_TTL_SECONDS:
                return _build_snapshot_result_locked(now_mono, from_cache=True)
        return None

    with _STATE_LOCK:
        _load_disk_history_locked(today_ist)
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
            trading_date, snap_ts, nse_ts_str, rows = _parse_oi_payload(payload)
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

        disk_snapshot: Optional[List[Tuple[float, str, Dict[str, Dict[str, Optional[float]]]]]] = None
        with _STATE_LOCK:
            _CONSECUTIVE_FAILURES = 0
            _COOLDOWN_UNTIL_MONO = 0.0
            _LAST_ERROR = None
            _LAST_FETCH_MONO = pytime.monotonic()

            if _HISTORY_DATE != trading_date:
                _OI_HISTORY.clear()
                _HISTORY_DATE = trading_date

            cutoff = snap_ts - _HISTORY_RETENTION_SECONDS
            while _OI_HISTORY and _OI_HISTORY[0][0] < cutoff:
                _OI_HISTORY.popleft()

            if not _OI_HISTORY or snap_ts > _OI_HISTORY[-1][0]:
                _OI_HISTORY.append((snap_ts, nse_ts_str, rows))
                disk_snapshot = list(_OI_HISTORY)

            result = _build_snapshot_result_locked(_LAST_FETCH_MONO, from_cache=False)

        if disk_snapshot is not None:
            _save_disk_history(trading_date, disk_snapshot)

        return result


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
