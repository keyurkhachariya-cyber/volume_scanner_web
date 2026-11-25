"""
Optimized relative-volume scanner logic using direct async Yahoo Chart API + yfinance fallback:
1. Compact NumPy-matrix disk & bounded in-memory baseline cache (O(1) vectorized lookup).
2. Multiplexed curl_cffi AsyncSession HTTP/2 fetching directly to NumPy arrays (zero DataFrame overhead on warm scans), with automatic yfinance fallback.
3. Single-pass coalesced cold baseline + today fetching via exact date bounds.
4. Thread-local persistent curl_cffi HTTP/2 sessions & ref-counted reusable worker pools for yfinance fallback.
"""

import asyncio
import atexit
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime, time, timedelta, date
import os
from pathlib import Path
import sys
import tempfile
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import quote as _url_quote
import pickle
import threading
import time as pytime

import numpy as np
import pandas as pd
import yfinance as yf
from yfinance.data import YfData

try:
    import orjson as _orjson
except ImportError:  # pragma: no cover - optional faster JSON decoder
    _orjson = None

try:
    from yfinance.multi import new_session as _yf_new_session
except ImportError:  # pragma: no cover - fallback for older yfinance versions
    _yf_new_session = None

from holidays import get_previous_trading_day, IST

MARKET_OPEN = time(9, 15)
_MARKET_OPEN_SEC = 9 * 3600 + 15 * 60
_NAT_INT64 = np.iinfo(np.int64).min
CACHE_DIR = Path(__file__).parent / ".cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_RETENTION_DAYS = 7
MEMORY_CACHE_MAX_ENTRIES = 4
BASELINE_FORMAT_VERSION = 2
_NEG_CACHE_TTL_SECONDS = 300.0

# Bounded LRU memory cache: (date, interval) -> BaselineStore (version 2)
_PREV_DAY_MEMORY_CACHE: "OrderedDict[Tuple[date, str], Dict[str, Any]]" = OrderedDict()
# Short-TTL in-memory negative cache: (date, interval) -> {symbol: expiry_monotonic}
_NEG_CACHE: Dict[Tuple[date, str], Dict[str, float]] = {}
_CACHE_LOCK = threading.Lock()

# Persistent thread-local HTTP sessions & reusable worker pools (kept warm across scans)
_SESSION_LOCK = threading.Lock()
_YF_PRIMING_DONE = False
_THREAD_LOCAL = threading.local()

_EXECUTOR_LOCK = threading.Lock()
_EXECUTORS: "OrderedDict[int, Dict[str, Any]]" = OrderedDict()
_MAX_POOLS = 4
_CHART_HOSTS = ("query1.finance.yahoo.com", "query2.finance.yahoo.com")

_ASYNC_LOOP_LOCK = threading.Lock()
_ASYNC_LOOP: Optional[asyncio.AbstractEventLoop] = None
_ASYNC_THREAD: Optional[threading.Thread] = None
_ASYNC_SESSION: Any = None
_ASYNC_CRUMB: Optional[str] = None
_ASYNC_CRUMB_LOCK: Optional[asyncio.Lock] = None


def _time_to_sec(t: time) -> int:
    return t.hour * 3600 + t.minute * 60 + t.second


def _ist_epoch_sec(d: date, t: time) -> int:
    """Convert an IST wall-clock (date, time) pair to UTC integer epoch seconds."""
    dt_naive = datetime.combine(d, t)
    localize = getattr(IST, "localize", None)
    dt_ist = localize(dt_naive) if localize is not None else dt_naive.replace(tzinfo=IST)
    return int(dt_ist.timestamp())


def _index_epoch_seconds(index: pd.DatetimeIndex) -> np.ndarray:
    """Convert DatetimeIndex to int64 UTC epoch seconds regardless of ns/us/ms/s resolution."""
    idx = index if index.tz is not None else index.tz_localize("UTC")
    as_unit = getattr(idx, "as_unit", None)
    if as_unit is not None:
        return as_unit("s").asi8
    return (idx.asi8 // 1_000_000_000).astype(np.int64, copy=False)


def _create_http_session() -> Any:
    """Create a browser-impersonating session compatible with installed yfinance."""
    if _yf_new_session is not None:
        return _yf_new_session()
    try:
        from curl_cffi import requests as curl_requests

        return curl_requests.Session(impersonate="chrome")
    except ImportError:
        import requests

        return requests.Session()


def _get_yf_session() -> Any:
    """Return a thread-local persistent session and prime yfinance's YfData singleton once."""
    global _YF_PRIMING_DONE
    session = getattr(_THREAD_LOCAL, "yf_session", None)
    if session is None:
        session = _create_http_session()
        _THREAD_LOCAL.yf_session = session
    if not _YF_PRIMING_DONE:
        with _SESSION_LOCK:
            if not _YF_PRIMING_DONE:
                YfData(session=session)
                _YF_PRIMING_DONE = True
    return session


@contextmanager
def _borrow_executor(max_workers: int):
    """Borrow a reusable ThreadPoolExecutor and defer shutting down evicted pools until idle."""
    workers = max(1, int(max_workers))
    with _EXECUTOR_LOCK:
        entry = _EXECUTORS.get(workers)
        if entry is None:
            entry = {
                "executor": ThreadPoolExecutor(max_workers=workers, thread_name_prefix=f"yf_scan_{workers}"),
                "active": 0,
                "retired": False,
            }
            _EXECUTORS[workers] = entry
        else:
            _EXECUTORS.move_to_end(workers)
        entry["active"] += 1

        while len(_EXECUTORS) > _MAX_POOLS:
            _, oldest = _EXECUTORS.popitem(last=False)
            oldest["retired"] = True
            if oldest["active"] == 0:
                oldest["executor"].shutdown(wait=False)

    try:
        yield entry["executor"]
    finally:
        should_shutdown = False
        with _EXECUTOR_LOCK:
            entry["active"] -= 1
            if entry["retired"] and entry["active"] == 0:
                should_shutdown = True
        if should_shutdown:
            entry["executor"].shutdown(wait=False)


def _prune_old_cache(max_age_days: int = CACHE_RETENTION_DAYS) -> None:
    """Remove stale .pkl files and legacy commit-1 prev_*.pkl cache artifacts."""
    cutoff_ts = datetime.now().timestamp() - (max_age_days * 86400)
    for path in CACHE_DIR.glob("*.pkl"):
        try:
            is_legacy_raw = path.name.startswith("prev_") and not path.name.startswith("prev_baseline_")
            if is_legacy_raw or path.stat().st_mtime < cutoff_ts:
                path.unlink(missing_ok=True)
        except Exception:
            pass


def prune_cache(max_age_days: int = CACHE_RETENTION_DAYS) -> None:
    """Public helper to prune expired cache files and legacy artifacts on startup."""
    _prune_old_cache(max_age_days=max_age_days)


def _standardize_index_tz(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure DataFrame index is localized/converted to IST without redundant copies."""
    if df.empty:
        return df
    tz = df.index.tz
    if tz is None:
        df.index = df.index.tz_localize("UTC").tz_convert(IST)
    elif getattr(tz, "zone", None) != "Asia/Kolkata" and str(tz) != "Asia/Kolkata":
        df.index = df.index.tz_convert(IST)
    return df


def _build_prev_day_baseline_from_arrays(
    ts_sec: np.ndarray,
    close_col: Optional[np.ndarray],
    vol_col: Optional[np.ndarray],
    target_date: Optional[date] = None,
) -> Optional[Dict[str, Any]]:
    """
    Pre-compute cumulative regular-hours volume arrays (sorted seconds-of-day)
    and previous close scalar directly from aligned NumPy arrays.
    """
    if vol_col is None or len(ts_sec) == 0:
        return None

    if target_date is not None:
        resolved_date = target_date
    else:
        valid_ts = ts_sec[ts_sec != _NAT_INT64]
        if len(valid_ts) == 0:
            return None
        resolved_date = datetime.fromtimestamp(int(valid_ts.max()), tz=IST).date()

    day_start_sec = _ist_epoch_sec(resolved_date, time(0, 0))
    day_end_sec = day_start_sec + 86400
    open_sec = day_start_sec + _MARKET_OPEN_SEC

    mask_date = (ts_sec >= day_start_sec) & (ts_sec < day_end_sec)
    if not mask_date.any():
        return None

    mask_reg = (ts_sec >= open_sec) & (ts_sec < day_end_sec)
    if not mask_reg.any():
        return None

    vol_arr = vol_col[mask_reg]
    has_vol = not np.isnan(vol_arr).all()
    has_close = close_col is not None and bool((~np.isnan(close_col[mask_reg])).any())
    if not has_vol and not has_close:
        return None

    secs = (ts_sec[mask_reg] - day_start_sec).astype(np.int32, copy=False)
    clean_vol = np.nan_to_num(vol_arr, nan=0.0)

    # Sort and aggregate duplicate timestamps if present
    order = np.argsort(secs, kind="mergesort")
    secs_sorted = secs[order]
    vol_sorted = clean_vol[order]

    if len(secs_sorted) > 1 and np.any(secs_sorted[1:] == secs_sorted[:-1]):
        uniq_secs, idx_start = np.unique(secs_sorted, return_index=True)
        vol_agg = np.add.reduceat(vol_sorted, idx_start)
        secs_sorted = uniq_secs
        cumvol = np.cumsum(vol_agg, dtype=np.float64)
    else:
        cumvol = np.cumsum(vol_sorted, dtype=np.float64)

    prev_close: Optional[float] = None
    if close_col is not None:
        close_date = close_col[mask_date]
        ts_date = ts_sec[mask_date]
        valid_mask = ~np.isnan(close_date)
        if valid_mask.any():
            valid_closes = close_date[valid_mask]
            valid_ts_date = ts_date[valid_mask]
            if len(valid_ts_date) > 1 and np.any(valid_ts_date[1:] < valid_ts_date[:-1]):
                c_order = np.argsort(valid_ts_date, kind="mergesort")
                prev_close = float(valid_closes[c_order[-1]])
            else:
                prev_close = float(valid_closes[-1])

    return {
        "times_sec": secs_sorted,
        "cumvol": cumvol,
        "prev_close": prev_close,
    }


def _build_prev_day_baseline(
    df_prev: pd.DataFrame, target_date: Optional[date] = None
) -> Optional[Dict[str, Any]]:
    """Pre-compute previous-day baseline from a pandas OHLCV DataFrame."""
    if df_prev is None or df_prev.empty or "Volume" not in df_prev.columns:
        return None

    ts_sec = _index_epoch_seconds(df_prev.index)
    vol_col = df_prev["Volume"].to_numpy(dtype=float, copy=False)
    close_col = df_prev["Close"].to_numpy(dtype=float, copy=False) if "Close" in df_prev.columns else None
    return _build_prev_day_baseline_from_arrays(ts_sec, close_col, vol_col, target_date=target_date)


def _empty_baseline_store(prev_day: date, interval: str) -> Dict[str, Any]:
    return {
        "version": BASELINE_FORMAT_VERSION,
        "prev_day": prev_day,
        "interval": interval,
        "times_sec": np.empty(0, dtype=np.int32),
        "symbols": [],
        "symbol_to_idx": {},
        "cumvol": np.empty((0, 0), dtype=np.float64),
        "prev_close": np.empty(0, dtype=np.float64),
        "prev_full_vol": np.empty(0, dtype=np.float64),
        "missing_symbols": set(),
    }


def _pack_baseline_store(
    entries: Dict[str, Optional[Dict[str, Any]]],
    prev_day: date,
    interval: str,
    existing: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Pack per-symbol baselines (and optional existing v2 store) into one contiguous 2D NumPy matrix."""
    merged_entries: Dict[str, Dict[str, Any]] = {}

    if existing and existing.get("version") == BASELINE_FORMAT_VERSION:
        ex_times = existing["times_sec"]
        ex_cumvol = existing["cumvol"]
        ex_close = existing["prev_close"]
        for col_idx, sym in enumerate(existing["symbols"]):
            col = ex_cumvol[:, col_idx]
            valid_mask = ~np.isnan(col)
            pc = float(ex_close[col_idx]) if not np.isnan(ex_close[col_idx]) else None
            merged_entries[sym] = {
                "times_sec": ex_times[valid_mask],
                "cumvol": col[valid_mask],
                "prev_close": pc,
            }

    for sym, entry in entries.items():
        if entry is not None:
            merged_entries[sym] = entry

    if not merged_entries:
        return _empty_baseline_store(prev_day, interval)

    symbols = sorted(merged_entries.keys())
    all_times = np.unique(np.concatenate([merged_entries[s]["times_sec"] for s in symbols]))
    num_t = len(all_times)
    num_s = len(symbols)

    mat = np.full((num_t, num_s), np.nan, dtype=np.float64)
    prev_close_arr = np.full(num_s, np.nan, dtype=np.float64)

    for col_idx, sym in enumerate(symbols):
        entry = merged_entries[sym]
        pos = np.searchsorted(all_times, entry["times_sec"])
        mat[pos, col_idx] = entry["cumvol"]
        if entry["prev_close"] is not None:
            prev_close_arr[col_idx] = entry["prev_close"]

    # Forward-fill cumulative volume across missing timestamps (leading NaNs stay NaN for exact .asof() parity)
    mat = pd.DataFrame(mat).ffill().to_numpy(dtype=np.float64, copy=False)
    prev_full_vol = np.nan_to_num(mat[-1, :], nan=0.0)

    return {
        "version": BASELINE_FORMAT_VERSION,
        "prev_day": prev_day,
        "interval": interval,
        "times_sec": all_times.astype(np.int32, copy=False),
        "symbols": symbols,
        "symbol_to_idx": {s: i for i, s in enumerate(symbols)},
        "cumvol": mat,
        "prev_close": prev_close_arr,
        "prev_full_vol": prev_full_vol,
        "missing_symbols": set(),
    }


def _upgrade_v1_baseline(
    raw_v1: Dict[str, Any], prev_day: date, interval: str
) -> Dict[str, Any]:
    """Convert legacy v1 dict ({sym: {'cumvol': pd.Series, 'prev_close': float}}) into v2 store."""
    entries: Dict[str, Optional[Dict[str, Any]]] = {}
    for sym, val in raw_v1.items():
        if val is None:
            continue
        series = val.get("cumvol")
        if series is None or series.empty:
            continue
        secs = np.fromiter((_time_to_sec(t) for t in series.index), dtype=np.int32, count=len(series))
        vals = series.to_numpy(dtype=np.float64, copy=True)
        order = np.argsort(secs, kind="mergesort")
        entries[sym] = {
            "times_sec": secs[order],
            "cumvol": vals[order],
            "prev_close": val.get("prev_close"),
        }
    return _pack_baseline_store(entries, prev_day, interval)


def _get_neg_cached(cache_key: Tuple[date, str]) -> Set[str]:
    now_mono = pytime.monotonic()
    with _CACHE_LOCK:
        bucket = _NEG_CACHE.get(cache_key)
        if not bucket:
            return set()
        expired = [sym for sym, exp in bucket.items() if now_mono >= exp]
        for sym in expired:
            del bucket[sym]
        return set(bucket.keys())


def _put_memory_cache_locked(cache_key: Tuple[date, str], store: Dict[str, Any]) -> None:
    _PREV_DAY_MEMORY_CACHE[cache_key] = store
    _PREV_DAY_MEMORY_CACHE.move_to_end(cache_key)
    while len(_PREV_DAY_MEMORY_CACHE) > MEMORY_CACHE_MAX_ENTRIES:
        evicted_key, _ = _PREV_DAY_MEMORY_CACHE.popitem(last=False)
        _NEG_CACHE.pop(evicted_key, None)


def _get_or_load_store(
    cache_key: Tuple[date, str], prev_day: date, interval: str
) -> Dict[str, Any]:
    """Load baseline store from LRU memory cache or disk under _CACHE_LOCK."""
    with _CACHE_LOCK:
        store = _PREV_DAY_MEMORY_CACHE.get(cache_key)
        if store is not None:
            _PREV_DAY_MEMORY_CACHE.move_to_end(cache_key)
            return store
        store = _load_disk_baseline(prev_day, interval)
        if store is None:
            store = _empty_baseline_store(prev_day, interval)
        _put_memory_cache_locked(cache_key, store)
        return store


def _load_disk_baseline(prev_day: date, interval: str) -> Optional[Dict[str, Any]]:
    cache_file = CACHE_DIR / f"prev_baseline_{prev_day.isoformat()}_{interval}.pkl"
    if not cache_file.exists():
        return None
    try:
        with open(cache_file, "rb") as f:
            loaded = pickle.load(f)
        if isinstance(loaded, dict) and loaded.get("version") == BASELINE_FORMAT_VERSION:
            if "symbol_to_idx" not in loaded:
                loaded["symbol_to_idx"] = {s: i for i, s in enumerate(loaded["symbols"])}
            # Never honor persisted negative-cache entries across sessions/loads
            loaded["missing_symbols"] = set()
            return loaded
        if isinstance(loaded, dict):
            upgraded = _upgrade_v1_baseline(loaded, prev_day, interval)
            _save_disk_baseline(prev_day, interval, upgraded)
            return upgraded
    except Exception:
        pass
    return None


def _save_disk_baseline(prev_day: date, interval: str, store: Dict[str, Any]) -> None:
    cache_file = CACHE_DIR / f"prev_baseline_{prev_day.isoformat()}_{interval}.pkl"
    tmp_path: Optional[Path] = None
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        # Omit symbol_to_idx and never persist negative-cache entries to disk
        disk_payload = {
            k: v for k, v in store.items() if k not in ("symbol_to_idx", "missing_symbols")
        }
        disk_payload["missing_symbols"] = set()
        fd, raw_tmp = tempfile.mkstemp(
            dir=str(CACHE_DIR),
            prefix=f".tmp_{prev_day.isoformat()}_{interval}_",
            suffix=".pkl",
        )
        tmp_path = Path(raw_tmp)
        with os.fdopen(fd, "wb") as f:
            pickle.dump(disk_payload, f, protocol=pickle.HIGHEST_PROTOCOL)
            f.flush()
        os.replace(tmp_path, cache_file)
    except Exception:
        if tmp_path is not None:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass


def _commit_baseline_updates(
    cache_key: Tuple[date, str],
    prev_day: date,
    interval: str,
    new_entries: Dict[str, Optional[Dict[str, Any]]],
    neg_syms: Iterable[str],
) -> Dict[str, Any]:
    """Synchronize negative-cache updates, merge against latest store, and persist atomically."""
    neg_list = list(neg_syms)
    now_mono = pytime.monotonic()
    with _CACHE_LOCK:
        if neg_list:
            exp = now_mono + _NEG_CACHE_TTL_SECONDS
            bucket = _NEG_CACHE.setdefault(cache_key, {})
            for sym in neg_list:
                bucket[sym] = exp
        if new_entries:
            bucket = _NEG_CACHE.get(cache_key)
            if bucket:
                for sym in new_entries:
                    bucket.pop(sym, None)

        latest = _PREV_DAY_MEMORY_CACHE.get(cache_key)
        if latest is None:
            latest = _load_disk_baseline(prev_day, interval)

        if new_entries:
            store = _pack_baseline_store(new_entries, prev_day, interval, existing=latest)
            _save_disk_baseline(prev_day, interval, store)
        elif latest is not None:
            store = latest
        else:
            store = _empty_baseline_store(prev_day, interval)

        _put_memory_cache_locked(cache_key, store)
        return store


def extract_ticker_df(batch_df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """Extract a single ticker's DataFrame from a batched yf.download result."""
    if batch_df.empty:
        return pd.DataFrame()

    try:
        if isinstance(batch_df.columns, pd.MultiIndex):
            if symbol in batch_df.columns.levels[0]:
                sub = batch_df[symbol].dropna(how="all")
                return _standardize_index_tz(sub)
            elif len(batch_df.columns.levels) > 1 and symbol in batch_df.columns.levels[1]:
                sub = batch_df.xs(symbol, axis=1, level=1).dropna(how="all")
                return _standardize_index_tz(sub)
        elif any(col in batch_df.columns for col in ("Open", "High", "Low", "Close", "Volume")):
            sub = batch_df.dropna(how="all").copy()
            return _standardize_index_tz(sub)
    except Exception:
        pass

    return pd.DataFrame()


def cumulative_volume_until(df: pd.DataFrame, cutoff: time, target_date: Optional[date] = None) -> float:
    """Sum volume of regular-hours bars for target_date (default: latest date in df) up to cutoff."""
    if df.empty or "Volume" not in df.columns:
        return 0.0

    df = _standardize_index_tz(df)
    resolved_date = target_date if target_date is not None else df.index.date.max()
    mask = (df.index.date == resolved_date) & (df.index.time >= MARKET_OPEN) & (df.index.time <= cutoff)
    vol = df.loc[mask, "Volume"].to_numpy(dtype=float, copy=False)
    return float(np.nansum(vol)) if len(vol) else 0.0


def _fetch_one_history(
    symbol: str,
    interval: str,
    period: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    timeout: int = 10,
) -> Tuple[str, Optional[pd.DataFrame], bool]:
    """
    Fetch OHLCV history for one ticker using the shared persistent session.
    Returns (symbol, df_or_none, succeeded_without_exception).
    """
    session = _get_yf_session()
    try:
        tkr = yf.Ticker(symbol, session=session)
        # Pre-seed NSE/BSE timezone to skip 218 SQLite/HTTP TZ lookups
        if symbol.endswith((".NS", ".BO")) and getattr(tkr, "_tz", None) is None:
            tkr._tz = "Asia/Kolkata"

        df = tkr.history(
            period=period,
            interval=interval,
            start=start,
            end=end,
            prepost=False,
            actions=False,
            auto_adjust=True,
            timeout=timeout,
        )
        if df is None or df.empty:
            ph = getattr(tkr, "_price_history", None)
            had_error = bool(ph is not None and getattr(ph, "_last_error", None))
            return symbol, None, not had_error

        return symbol, _standardize_index_tz(df), True
    except Exception:
        return symbol, None, False


def _fetch_histories_concurrent(
    symbol_periods: Iterable[Tuple[str, Optional[str]]],
    interval: str,
    max_workers: int = 8,
    start: Optional[str] = None,
    end: Optional[str] = None,
    chunk_size: Optional[int] = None,
    symbol_ranges: Optional[Dict[str, Tuple[Optional[str], Optional[str]]]] = None,
) -> Tuple[Dict[str, pd.DataFrame], Set[str]]:
    """
    Concurrently fetch ticker histories over a single reusable worker pool without chunk barriers.
    Returns (symbol_dfs, failed_symbols).
    """
    tasks = list(symbol_periods)
    if not tasks:
        return {}, set()

    _get_yf_session()  # Prime YfData once before parallel fan-out

    # Optional explicit chunking only if a caller specifically requests chunk_size > 0
    if chunk_size and chunk_size > 0 and len(tasks) > chunk_size:
        batches = [tasks[i : i + chunk_size] for i in range(0, len(tasks), chunk_size)]
    else:
        batches = [tasks]

    results: Dict[str, pd.DataFrame] = {}
    failed: Set[str] = set()

    def _range_for(sym: str) -> Tuple[Optional[str], Optional[str]]:
        if symbol_ranges and sym in symbol_ranges:
            return symbol_ranges[sym]
        return start, end

    with _borrow_executor(max_workers) as executor:
        for batch in batches:
            futures = [
                executor.submit(_fetch_one_history, sym, interval, period, *_range_for(sym))
                for sym, period in batch
            ]
            for fut in as_completed(futures):
                sym, df, ok = fut.result()
                if df is not None and not df.empty:
                    results[sym] = df
                elif not ok:
                    failed.add(sym)

    # Single reduced-concurrency backoff retry for transient failures (e.g. 429/timeout)
    if failed:
        period_by_sym = dict(tasks)
        retry_syms = sorted(failed)
        pytime.sleep(0.5)
        with _borrow_executor(min(2, max(1, int(max_workers)))) as retry_executor:
            retry_futures = [
                retry_executor.submit(
                    _fetch_one_history,
                    sym,
                    interval,
                    period_by_sym.get(sym),
                    *_range_for(sym),
                )
                for sym in retry_syms
            ]
            for fut in as_completed(retry_futures):
                sym, df, ok = fut.result()
                if df is not None and not df.empty:
                    results[sym] = df
                    failed.discard(sym)
                elif ok:
                    failed.discard(sym)

    return results, failed


def get_previous_day_data(
    symbols: List[str],
    prev_day: date,
    interval: str = "1m",
    chunk_size: Optional[int] = None,
    max_workers: int = 8,
) -> Dict[str, Any]:
    """
    Fetch/load previous trading day's compact BaselineStore (v2 matrix)
    with bounded in-memory LRU cache, disk cache, and short-TTL memory negative cache.
    """
    cache_key = (prev_day, interval)
    store = _get_or_load_store(cache_key, prev_day, interval)

    known_set = set(store["symbols"]) | _get_neg_cached(cache_key)
    missing = [sym for sym in symbols if sym not in known_set]

    if missing:
        _prune_old_cache()
        start_str = prev_day.strftime("%Y-%m-%d")
        end_str = (prev_day + timedelta(days=1)).strftime("%Y-%m-%d")

        fetched, failed = _fetch_market_data(
            ((sym, None) for sym in missing),
            interval=interval,
            max_workers=max_workers,
            start=start_str,
            end=end_str,
            chunk_size=chunk_size,
        )

        new_entries: Dict[str, Optional[Dict[str, Any]]] = {}
        neg_syms: List[str] = []
        for sym in missing:
            if sym in failed:
                continue
            bundle = fetched.get(sym)
            baseline = (
                _build_prev_day_baseline_from_arrays(
                    bundle["ts_sec"], bundle["close"], bundle["volume"], target_date=prev_day
                )
                if bundle is not None
                else None
            )
            if baseline is not None:
                new_entries[sym] = baseline
            else:
                neg_syms.append(sym)

        store = _commit_baseline_updates(cache_key, prev_day, interval, new_entries, neg_syms)
        return store

    return _commit_baseline_updates(cache_key, prev_day, interval, {}, [])


def get_today_data(
    symbols: List[str],
    today: date,
    interval: str = "1m",
    chunk_size: Optional[int] = None,
    max_workers: int = 8,
) -> Dict[str, pd.DataFrame]:
    """Fetch today's intraday bars concurrently using period='1d' (no TZ lookup)."""
    fetched, _ = _fetch_histories_concurrent(
        ((sym, "1d") for sym in symbols),
        interval=interval,
        max_workers=max_workers,
        chunk_size=chunk_size,
    )
    today_data: Dict[str, pd.DataFrame] = {}
    for sym, df in fetched.items():
        mask = df.index.date == today
        if mask.any():
            today_data[sym] = df.loc[mask]
    return today_data


def _df_to_array_bundle(df: pd.DataFrame) -> Optional[Dict[str, Optional[np.ndarray]]]:
    """Convert an OHLCV DataFrame into a zero-copy NumPy array bundle."""
    if df is None or df.empty:
        return None
    cols = df.columns
    return {
        "ts_sec": _index_epoch_seconds(df.index),
        "open": df["Open"].to_numpy(dtype=float, copy=False) if "Open" in cols else None,
        "high": df["High"].to_numpy(dtype=float, copy=False) if "High" in cols else None,
        "low": df["Low"].to_numpy(dtype=float, copy=False) if "Low" in cols else None,
        "close": df["Close"].to_numpy(dtype=float, copy=False) if "Close" in cols else None,
        "volume": df["Volume"].to_numpy(dtype=float, copy=False) if "Volume" in cols else None,
    }


def _parse_yahoo_chart_payload(payload: Any) -> Tuple[Optional[Dict[str, Optional[np.ndarray]]], bool]:
    """
    Parse a Yahoo v8/finance/chart JSON payload into NumPy arrays without a DataFrame.
    Returns (array_bundle_or_none, succeeded_without_transient_error).
    """
    if not isinstance(payload, dict):
        return None, False
    chart = payload.get("chart")
    if not isinstance(chart, dict):
        return None, False

    err = chart.get("error")
    if err:
        code = err.get("code") if isinstance(err, dict) else None
        return None, code == "Not Found"

    results = chart.get("result")
    if not results or not isinstance(results, list):
        return None, True
    res0 = results[0]
    if not isinstance(res0, dict):
        return None, False

    timestamps = res0.get("timestamp")
    indicators = res0.get("indicators")
    if not timestamps or not isinstance(indicators, dict):
        # Empty day / pre-market response with no chart error
        return None, True

    quotes = indicators.get("quote")
    if not quotes or not isinstance(quotes, list) or not isinstance(quotes[0], dict):
        return None, True
    q0 = quotes[0]

    n = len(timestamps)
    if n == 0:
        return None, True

    try:
        ts_sec = np.asarray(timestamps, dtype=np.int64)
    except (TypeError, ValueError):
        ts_sec = np.array(
            [_NAT_INT64 if t is None else int(t) for t in timestamps],
            dtype=np.int64,
        )

    def _to_float_arr(key: str) -> Optional[np.ndarray]:
        raw = q0.get(key)
        if not raw or len(raw) != n:
            return None
        return np.asarray(raw, dtype=np.float64)

    return {
        "ts_sec": ts_sec,
        "open": _to_float_arr("open"),
        "high": _to_float_arr("high"),
        "low": _to_float_arr("low"),
        "close": _to_float_arr("close"),
        "volume": _to_float_arr("volume"),
    }, True


def _decode_json_response(resp: Any) -> Any:
    raw_content = getattr(resp, "content", None)
    if _orjson is not None and raw_content is not None:
        return _orjson.loads(raw_content)
    return resp.json()


def _shutdown_async_runtime() -> None:
    """Best-effort cleanup of persistent AsyncSession and event loop on interpreter exit."""
    global _ASYNC_SESSION, _ASYNC_LOOP, _ASYNC_THREAD
    with _ASYNC_LOOP_LOCK:
        loop = _ASYNC_LOOP
        session = _ASYNC_SESSION
        thread = _ASYNC_THREAD
        _ASYNC_LOOP = None
        _ASYNC_SESSION = None
        _ASYNC_THREAD = None
    if loop is None or loop.is_closed():
        return
    try:
        if session is not None:
            fut = asyncio.run_coroutine_threadsafe(session.close(), loop)
            fut.result(timeout=2.0)
    except Exception:
        pass
    try:
        loop.call_soon_threadsafe(loop.stop)
        if thread is not None and thread.is_alive():
            thread.join(timeout=2.0)
    except Exception:
        pass


atexit.register(_shutdown_async_runtime)


def _ensure_async_loop() -> asyncio.AbstractEventLoop:
    """Return a single long-lived background event-loop thread (SelectorEventLoop on Windows)."""
    global _ASYNC_LOOP, _ASYNC_THREAD
    with _ASYNC_LOOP_LOCK:
        if (
            _ASYNC_LOOP is not None
            and not _ASYNC_LOOP.is_closed()
            and _ASYNC_THREAD is not None
            and _ASYNC_THREAD.is_alive()
        ):
            return _ASYNC_LOOP

        ready = threading.Event()
        holder: Dict[str, Any] = {}

        def _loop_main() -> None:
            loop = asyncio.SelectorEventLoop() if sys.platform == "win32" else asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            holder["loop"] = loop
            loop.call_soon(ready.set)
            try:
                loop.run_forever()
            finally:
                try:
                    loop.run_until_complete(loop.shutdown_asyncgens())
                except Exception:
                    pass
                asyncio.set_event_loop(None)
                loop.close()

        t = threading.Thread(target=_loop_main, name="yf_async_loop", daemon=True)
        t.start()
        ready.wait(timeout=5.0)
        loop = holder.get("loop")
        if loop is None:
            raise RuntimeError("Failed to start persistent async event loop")
        _ASYNC_LOOP = loop
        _ASYNC_THREAD = t
        return loop


def _run_coro_sync(coro_fn):
    """Run an async coroutine factory on the persistent background event loop thread."""
    loop = _ensure_async_loop()
    fut = asyncio.run_coroutine_threadsafe(coro_fn(), loop)
    return fut.result()


async def _fetch_charts_async(
    tasks: List[Tuple[str, Optional[str]]],
    interval: str,
    max_concurrency: int,
    symbol_ranges: Optional[Dict[str, Tuple[Optional[str], Optional[str]]]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    timeout: int = 10,
) -> Tuple[Dict[str, Dict[str, Optional[np.ndarray]]], Set[str]]:
    """
    Concurrently fetch Yahoo Chart API JSON responses over a persistent multiplexed
    curl_cffi.requests.AsyncSession, load-balancing query1/query2 and caching cookie/crumb.
    """
    global _ASYNC_SESSION, _ASYNC_CRUMB, _ASYNC_CRUMB_LOCK
    from curl_cffi.requests import AsyncSession

    # Hoist date parsing / IST epoch conversion once per distinct (start, end) pair
    range_epoch_cache: Dict[Tuple[str, str], Tuple[int, int]] = {}

    def _epoch_bounds(s_str: str, e_str: str) -> Tuple[int, int]:
        key = (s_str, e_str)
        cached = range_epoch_cache.get(key)
        if cached is not None:
            return cached
        bounds = (
            _ist_epoch_sec(date.fromisoformat(s_str), time(0, 0)),
            _ist_epoch_sec(date.fromisoformat(e_str), time(0, 0)),
        )
        range_epoch_cache[key] = bounds
        return bounds

    def _build_params(sym: str, period: Optional[str], crumb: Optional[str]) -> Dict[str, Any]:
        s_str, e_str = symbol_ranges.get(sym, (start, end)) if symbol_ranges else (start, end)
        params: Dict[str, Any] = {"interval": interval, "includePrePost": "false"}
        if s_str and e_str:
            p1, p2 = _epoch_bounds(s_str, e_str)
            params["period1"] = p1
            params["period2"] = p2
        else:
            params["range"] = period or "1d"
        if crumb:
            params["crumb"] = crumb
        return params

    results: Dict[str, Dict[str, Optional[np.ndarray]]] = {}
    failed: Set[str] = set()
    period_by_sym = dict(tasks)
    sym_host_idx = {sym: idx for idx, (sym, _) in enumerate(tasks)}

    try:
        running_loop = asyncio.get_running_loop()
    except RuntimeError:
        running_loop = None

    persistent = _ASYNC_LOOP is not None and running_loop is _ASYNC_LOOP
    local_session = None
    local_crumb_state: Dict[str, Optional[str]] = {"value": None}

    if persistent:
        if _ASYNC_SESSION is None:
            _ASYNC_SESSION = AsyncSession(impersonate="chrome")
        if _ASYNC_CRUMB_LOCK is None:
            _ASYNC_CRUMB_LOCK = asyncio.Lock()
        session = _ASYNC_SESSION
        crumb_lock = _ASYNC_CRUMB_LOCK
    else:
        local_session = AsyncSession(impersonate="chrome")
        session = local_session
        crumb_lock = asyncio.Lock()

    async def _ensure_crumb(failed_crumb: Optional[str] = None) -> Optional[str]:
        global _ASYNC_CRUMB
        async with crumb_lock:
            cur = _ASYNC_CRUMB if persistent else local_crumb_state["value"]
            if cur and cur != failed_crumb:
                return cur
            try:
                await session.get("https://fc.yahoo.com", timeout=timeout)
                resp = await session.get(
                    f"https://{_CHART_HOSTS[0]}/v1/test/getcrumb",
                    timeout=timeout,
                )
                if resp.status_code == 200:
                    text = (resp.text or "").strip()
                    if text and "<html" not in text.lower():
                        if persistent:
                            _ASYNC_CRUMB = text
                        else:
                            local_crumb_state["value"] = text
                        return text
            except Exception:
                pass
            return _ASYNC_CRUMB if persistent else local_crumb_state["value"]

    async def _fetch_one(
        sym: str,
        period: Optional[str],
        sem: asyncio.Semaphore,
        host_idx: int = 0,
        allow_auth_retry: bool = True,
    ) -> Tuple[str, Optional[Dict[str, Optional[np.ndarray]]], bool]:
        host = _CHART_HOSTS[host_idx % len(_CHART_HOSTS)]
        url = f"https://{host}/v8/finance/chart/{_url_quote(sym, safe='')}"
        async with sem:
            try:
                used_crumb = _ASYNC_CRUMB if persistent else local_crumb_state["value"]
                resp = await session.get(
                    url,
                    params=_build_params(sym, period, used_crumb),
                    timeout=timeout,
                )
                if resp.status_code in (401, 403) and allow_auth_retry:
                    crumb = await _ensure_crumb(failed_crumb=used_crumb)
                    if crumb:
                        resp = await session.get(
                            url,
                            params=_build_params(sym, period, crumb),
                            timeout=timeout,
                        )
                if resp.status_code == 404:
                    try:
                        _, ok = _parse_yahoo_chart_payload(_decode_json_response(resp))
                    except Exception:
                        ok = False
                    return sym, None, ok
                if resp.status_code != 200:
                    return sym, None, False
                bundle, ok = _parse_yahoo_chart_payload(_decode_json_response(resp))
                return sym, bundle, ok
            except Exception:
                return sym, None, False

    try:
        if local_session is not None:
            await local_session.__aenter__()

        sem = asyncio.Semaphore(max(1, int(max_concurrency)))
        wave1 = await asyncio.gather(
            *(_fetch_one(sym, period, sem, host_idx=idx) for idx, (sym, period) in enumerate(tasks))
        )
        for sym, bundle, ok in wave1:
            if bundle is not None:
                results[sym] = bundle
            elif not ok:
                failed.add(sym)

        if failed:
            retry_syms = sorted(failed)
            await asyncio.sleep(0.5)
            retry_sem = asyncio.Semaphore(min(4, max(1, int(max_concurrency) // 4)))
            wave2 = await asyncio.gather(
                *(
                    _fetch_one(
                        sym,
                        period_by_sym.get(sym),
                        retry_sem,
                        host_idx=sym_host_idx.get(sym, 0) + 1,
                    )
                    for sym in retry_syms
                )
            )
            for sym, bundle, ok in wave2:
                if bundle is not None:
                    results[sym] = bundle
                    failed.discard(sym)
                elif ok:
                    failed.discard(sym)
    finally:
        if local_session is not None:
            await local_session.__aexit__(None, None, None)

    return results, failed


def _fetch_market_data(
    symbol_periods: Iterable[Tuple[str, Optional[str]]],
    interval: str,
    max_workers: int = 8,
    start: Optional[str] = None,
    end: Optional[str] = None,
    chunk_size: Optional[int] = None,
    symbol_ranges: Optional[Dict[str, Tuple[Optional[str], Optional[str]]]] = None,
) -> Tuple[Dict[str, Dict[str, Optional[np.ndarray]]], Set[str]]:
    """
    Fast path: fetch Yahoo Chart API directly via persistent curl_cffi AsyncSession into NumPy arrays.
    Fallback path: transparently falls back to yfinance for any remaining failed symbols
    (or if curl_cffi AsyncSession is unavailable/unsupported).
    """
    tasks = list(symbol_periods)
    if not tasks:
        return {}, set()

    arrays_by_sym: Dict[str, Dict[str, Optional[np.ndarray]]] = {}
    fallback_syms: Set[str] = set()

    try:
        from curl_cffi.requests import AsyncSession as _unused  # noqa: F401
        has_async = True
    except ImportError:
        has_async = False

    if has_async:
        async_concurrency = min(24, max(8, int(max_workers) * 2))
        try:
            arrays_by_sym, fallback_syms = _run_coro_sync(
                lambda: _fetch_charts_async(
                    tasks=tasks,
                    interval=interval,
                    max_concurrency=async_concurrency,
                    symbol_ranges=symbol_ranges,
                    start=start,
                    end=end,
                )
            )
        except Exception:
            fallback_syms = {sym for sym, _ in tasks}
    else:
        fallback_syms = {sym for sym, _ in tasks}

    if fallback_syms:
        period_by_sym = dict(tasks)
        fb_tasks = [(sym, period_by_sym.get(sym)) for sym in _tasks_order(tasks, fallback_syms)]
        fb_ranges = (
            {sym: symbol_ranges[sym] for sym in fallback_syms if sym in symbol_ranges}
            if symbol_ranges
            else None
        )
        fb_dfs, fb_failed = _fetch_histories_concurrent(
            fb_tasks,
            interval=interval,
            max_workers=max_workers,
            start=start,
            end=end,
            chunk_size=chunk_size,
            symbol_ranges=fb_ranges,
        )
        for sym, df in fb_dfs.items():
            bundle = _df_to_array_bundle(df)
            if bundle is not None:
                arrays_by_sym[sym] = bundle
        return arrays_by_sym, fb_failed

    return arrays_by_sym, set()


def _tasks_order(tasks: List[Tuple[str, Optional[str]]], subset: Set[str]) -> List[str]:
    return [sym for sym, _ in tasks if sym in subset]


def _summarize_today_arrays(
    ts_sec: np.ndarray,
    open_col: Optional[np.ndarray],
    high_col: Optional[np.ndarray],
    low_col: Optional[np.ndarray],
    close_col: Optional[np.ndarray],
    vol_col: Optional[np.ndarray],
    today: date,
    cutoff: time,
    day_start_sec: Optional[int] = None,
    cutoff_bound_sec: Optional[int] = None,
) -> Optional[Tuple[float, Optional[float], Optional[float], Optional[float], Optional[float], Optional[time]]]:
    """Fast NumPy summary of today's regular-hours (vol_today, ltp, open, high, low, last_bar_time)."""
    if len(ts_sec) == 0:
        return None

    if day_start_sec is None:
        day_start_sec = _ist_epoch_sec(today, time(0, 0))
    open_sec = day_start_sec + _MARKET_OPEN_SEC
    if cutoff_bound_sec is None:
        cutoff_bound_sec = day_start_sec + _time_to_sec(cutoff)

    mask_reg = (ts_sec >= open_sec) & (ts_sec <= cutoff_bound_sec)
    if not mask_reg.any():
        return None

    t_reg = ts_sec[mask_reg]
    open_arr = open_col[mask_reg] if open_col is not None else None
    high_arr = high_col[mask_reg] if high_col is not None else None
    low_arr = low_col[mask_reg] if low_col is not None else None
    close_arr = close_col[mask_reg] if close_col is not None else None
    vol_arr = vol_col[mask_reg] if vol_col is not None else None

    # Drop placeholder rows with neither valid OHLC price nor positive volume
    valid_row = np.zeros(len(t_reg), dtype=bool)
    for arr in (open_arr, high_arr, low_arr, close_arr):
        if arr is not None:
            valid_row |= ~np.isnan(arr)
    if vol_arr is not None:
        valid_row |= np.nan_to_num(vol_arr, nan=0.0) > 0.0
    if not valid_row.any():
        return None

    if not valid_row.all():
        t_reg = t_reg[valid_row]
        open_arr = open_arr[valid_row] if open_arr is not None else None
        high_arr = high_arr[valid_row] if high_arr is not None else None
        low_arr = low_arr[valid_row] if low_arr is not None else None
        close_arr = close_arr[valid_row] if close_arr is not None else None
        vol_arr = vol_arr[valid_row] if vol_arr is not None else None

    if len(t_reg) > 1 and np.any(t_reg[1:] < t_reg[:-1]):
        order = np.argsort(t_reg, kind="mergesort")
        t_reg = t_reg[order]
        open_arr = open_arr[order] if open_arr is not None else None
        high_arr = high_arr[order] if high_arr is not None else None
        low_arr = low_arr[order] if low_arr is not None else None
        close_arr = close_arr[order] if close_arr is not None else None
        vol_arr = vol_arr[order] if vol_arr is not None else None

    last_bar_sec = int(t_reg[-1] - day_start_sec)
    last_bar_time = time(last_bar_sec // 3600, (last_bar_sec % 3600) // 60, last_bar_sec % 60)

    if vol_arr is not None and len(vol_arr):
        if len(t_reg) > 1 and np.any(t_reg[1:] == t_reg[:-1]):
            _, idx_start = np.unique(t_reg, return_index=True)
            vol_dedup = np.maximum.reduceat(np.nan_to_num(vol_arr, nan=0.0), idx_start)
            vol_today = float(np.sum(vol_dedup, dtype=np.float64))
        else:
            vol_today = float(np.nansum(vol_arr))
    else:
        vol_today = 0.0

    last_price: Optional[float] = None
    if close_arr is not None:
        valid = close_arr[~np.isnan(close_arr)]
        if len(valid):
            last_price = float(valid[-1])

    day_open: Optional[float] = None
    if open_arr is not None:
        valid = open_arr[~np.isnan(open_arr)]
        if len(valid):
            day_open = float(valid[0])

    day_high: Optional[float] = None
    if high_arr is not None and not np.isnan(high_arr).all():
        day_high = float(np.nanmax(high_arr))

    day_low: Optional[float] = None
    if low_arr is not None and not np.isnan(low_arr).all():
        day_low = float(np.nanmin(low_arr))

    return vol_today, last_price, day_open, day_high, day_low, last_bar_time


def _summarize_today_df(
    df: pd.DataFrame,
    today: date,
    cutoff: time,
    day_start_sec: Optional[int] = None,
    cutoff_bound_sec: Optional[int] = None,
) -> Optional[Tuple[float, Optional[float], Optional[float], Optional[float], Optional[float], Optional[time]]]:
    """Summarize today's regular-hours metrics from a pandas OHLCV DataFrame."""
    bundle = _df_to_array_bundle(df)
    if bundle is None:
        return None
    return _summarize_today_arrays(
        bundle["ts_sec"],
        bundle["open"],
        bundle["high"],
        bundle["low"],
        bundle["close"],
        bundle["volume"],
        today,
        cutoff,
        day_start_sec=day_start_sec,
        cutoff_bound_sec=cutoff_bound_sec,
    )


def run_scan(
    symbols: List[str],
    min_ratio: float = 1.5,
    min_volume: int = 0,
    interval: str = "1m",
    max_workers: int = 8,
    chunk_size: Optional[int] = None,
    prev_day: Optional[date] = None,
) -> pd.DataFrame:
    """
    Run relative volume scan across symbols using:
    - Compact NumPy baseline cache (O(1) vectorized lookup)
    - Persistent curl_cffi AsyncSession HTTP/2 Chart API fetching directly to NumPy arrays (with yfinance fallback)
    - Single-pass coalesced cold baseline + today fetching via exact date bounds
    - Per-ticker bar-aligned comparison (eliminates look-ahead bias on lagging Yahoo feeds)
    Pass min_ratio=0.0 and min_volume=0 to retrieve all valid symbols for client-side filtering.
    """
    if not symbols:
        return pd.DataFrame()

    now = datetime.now(IST)
    cutoff = now.time()
    cutoff_sec = _time_to_sec(cutoff)
    today = now.date()
    if prev_day is None:
        prev_day = get_previous_trading_day(today)
    cache_key = (prev_day, interval)

    # 1. Load existing baseline store (memory LRU -> disk) under lock
    store = _get_or_load_store(cache_key, prev_day, interval)

    neg_set = _get_neg_cached(cache_key)
    known_set = set(store["symbols"]) | neg_set
    missing = [sym for sym in symbols if sym not in known_set]
    missing_set = set(missing)

    gap_days = (today - prev_day).days
    can_coalesce_range = 0 < gap_days <= (6 if interval == "1m" else 55)

    # 2. Fetch histories in one concurrent wave (coalescing cold prev_day..today via exact start/end)
    if missing and not can_coalesce_range:
        # Rare >6-day holiday gap on 1m bars (or >55d on 5m): fetch prev_day explicitly first
        store = get_previous_day_data(
            symbols, prev_day, interval=interval, chunk_size=chunk_size, max_workers=max_workers
        )
        missing_set = set()
        neg_set = _get_neg_cached(cache_key)

    # Skip network fetches for symbols sitting in negative cache without any baseline entry
    skip_today_set = neg_set - set(store["symbol_to_idx"].keys())
    prev_day_str = prev_day.strftime("%Y-%m-%d")
    tomorrow_str = (today + timedelta(days=1)).strftime("%Y-%m-%d")
    symbol_ranges: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
    symbol_periods: List[Tuple[str, Optional[str]]] = []
    for sym in symbols:
        if sym in skip_today_set:
            continue
        if sym in missing_set and can_coalesce_range:
            symbol_periods.append((sym, None))
            symbol_ranges[sym] = (prev_day_str, tomorrow_str)
        else:
            symbol_periods.append((sym, "1d"))

    fetched_arrays, failed_syms = _fetch_market_data(
        symbol_periods,
        interval=interval,
        max_workers=max_workers,
        chunk_size=chunk_size,
        symbol_ranges=symbol_ranges if symbol_ranges else None,
    )

    # 3. Extract & persist any newly fetched previous-day baselines from coalesced responses
    if missing_set:
        _prune_old_cache()
        new_entries: Dict[str, Optional[Dict[str, Any]]] = {}
        neg_syms: List[str] = []
        fallback_missing: List[str] = []
        prev_day_start_sec = _ist_epoch_sec(prev_day, time(0, 0))

        for sym in missing:
            if sym in failed_syms:
                continue
            bundle = fetched_arrays.get(sym)
            if bundle is None or len(bundle["ts_sec"]) == 0:
                neg_syms.append(sym)
                continue
            baseline = _build_prev_day_baseline_from_arrays(
                bundle["ts_sec"], bundle["close"], bundle["volume"], target_date=prev_day
            )
            if baseline is not None:
                new_entries[sym] = baseline
            else:
                ts_sym = bundle["ts_sec"]
                valid_ts = ts_sym[ts_sym != _NAT_INT64]
                if len(valid_ts) > 0 and (valid_ts < prev_day_start_sec).any():
                    neg_syms.append(sym)
                else:
                    fallback_missing.append(sym)

        if fallback_missing:
            start_str = prev_day.strftime("%Y-%m-%d")
            end_str = (prev_day + timedelta(days=1)).strftime("%Y-%m-%d")
            fb_arrays, fb_failed = _fetch_market_data(
                ((sym, None) for sym in fallback_missing),
                interval=interval,
                max_workers=max_workers,
                start=start_str,
                end=end_str,
                chunk_size=chunk_size,
            )
            for sym in fallback_missing:
                if sym in fb_failed:
                    continue
                fb_bundle = fb_arrays.get(sym)
                baseline = (
                    _build_prev_day_baseline_from_arrays(
                        fb_bundle["ts_sec"], fb_bundle["close"], fb_bundle["volume"], target_date=prev_day
                    )
                    if fb_bundle is not None
                    else None
                )
                if baseline is not None:
                    new_entries[sym] = baseline
                else:
                    neg_syms.append(sym)

        store = _commit_baseline_updates(cache_key, prev_day, interval, new_entries, neg_syms)

    # 4. Per-ticker bar-aligned baseline lookup (prevents look-ahead bias on lagging feeds)
    times_sec = store["times_sec"]
    if len(times_sec) == 0 or cutoff_sec < int(times_sec[0]):
        return pd.DataFrame()

    today_day_start_sec = _ist_epoch_sec(today, time(0, 0))
    today_cutoff_bound_sec = today_day_start_sec + cutoff_sec
    cumvol_mat = store["cumvol"]
    prev_close_arr = store["prev_close"]
    prev_full_arr = store["prev_full_vol"]
    sym_to_idx = store["symbol_to_idx"]
    row_idx_cache: Dict[int, int] = {}

    results = []
    for sym in symbols:
        col_idx = sym_to_idx.get(sym)
        if col_idx is None:
            continue

        bundle = fetched_arrays.get(sym)
        summary = (
            _summarize_today_arrays(
                bundle["ts_sec"],
                bundle["open"],
                bundle["high"],
                bundle["low"],
                bundle["close"],
                bundle["volume"],
                today,
                cutoff,
                today_day_start_sec,
                today_cutoff_bound_sec,
            )
            if bundle is not None
            else None
        )
        if summary is None:
            continue

        vol_today, last_price, day_open, day_high, day_low, last_bar_time = summary
        if last_bar_time is None:
            continue

        aligned_time = last_bar_time if last_bar_time <= cutoff else cutoff
        aligned_sec = _time_to_sec(aligned_time)
        t_idx = row_idx_cache.get(aligned_sec)
        if t_idx is None:
            t_idx = int(np.searchsorted(times_sec, aligned_sec, side="right")) - 1
            row_idx_cache[aligned_sec] = t_idx
        if t_idx < 0:
            continue

        vol_prev = float(cumvol_mat[t_idx, col_idx])
        if np.isnan(vol_prev) or vol_prev <= 0.0:
            continue

        ratio = vol_today / vol_prev
        if ratio < min_ratio or vol_today < min_volume:
            continue

        raw_pc = float(prev_close_arr[col_idx])
        prev_close = None if np.isnan(raw_pc) else raw_pc
        prev_full_vol = float(prev_full_arr[col_idx])

        range_pct = None
        if day_high is not None and day_low is not None and day_low > 0:
            range_pct = ((day_high - day_low) / day_low) * 100

        gap_pct = None
        pct_change = None
        if prev_close is not None and prev_close > 0:
            if day_open is not None:
                gap_pct = ((day_open - prev_close) / prev_close) * 100
            if last_price is not None:
                pct_change = ((last_price - prev_close) / prev_close) * 100

        vol_diff = vol_today - vol_prev
        turnover_cr = ((last_price * vol_today) / 1e7) if last_price is not None else None

        results.append(
            {
                "Symbol": sym.removesuffix(".NS"),
                "LTP": round(last_price, 2) if last_price is not None else None,
                "Vol_Today": int(vol_today),
                "Vol_Prev_Same_Time": int(vol_prev),
                "Ratio": round(ratio, 2),
                "Pct_Change": round(pct_change, 2) if pct_change is not None else None,
                "Last_Bar_Time": aligned_time.strftime("%H:%M"),
                "Gap_Pct": round(gap_pct, 2) if gap_pct is not None else None,
                "Prev_Close": round(prev_close, 2) if prev_close is not None else None,
                "Day_Open": round(day_open, 2) if day_open is not None else None,
                "Day_High": round(day_high, 2) if day_high is not None else None,
                "Day_Low": round(day_low, 2) if day_low is not None else None,
                "Range_Pct": round(range_pct, 2) if range_pct is not None else None,
                "Vol_Diff": int(vol_diff),
                "Prev_Full_Vol": int(prev_full_vol),
                "Turnover_Cr": round(turnover_cr, 2) if turnover_cr is not None else None,
            }
        )

    if not results:
        return pd.DataFrame()

    df_out = pd.DataFrame(results)
    df_out = df_out.sort_values("Ratio", ascending=False).reset_index(drop=True)
    return df_out
