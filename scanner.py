"""
Optimized relative-volume scanner logic using yfinance with:
1. Compact NumPy-matrix disk & bounded in-memory baseline cache (O(1) vectorized lookup).
2. Persistent curl_cffi HTTP/2 session & reusable bounded ThreadPoolExecutor.
3. Single-pass period-based history fetching (avoids per-ticker TZ lookups & coalesces cold baseline + today fetches).
4. Fast NumPy-based OHLCV extraction without wide MultiIndex pd.concat overhead.
"""

from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime, time, timedelta, date
import os
from pathlib import Path
import tempfile
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
import pickle
import threading
import time as pytime

import numpy as np
import pandas as pd
import yfinance as yf
from yfinance.data import YfData

try:
    from yfinance.multi import new_session as _yf_new_session
except ImportError:  # pragma: no cover - fallback for older yfinance versions
    _yf_new_session = None

from holidays import get_previous_trading_day, IST

MARKET_OPEN = time(9, 15)
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


def _time_to_sec(t: time) -> int:
    return t.hour * 3600 + t.minute * 60 + t.second


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


def _build_prev_day_baseline(
    df_prev: pd.DataFrame, target_date: Optional[date] = None
) -> Optional[Dict[str, Any]]:
    """
    Pre-compute cumulative regular-hours volume arrays (sorted seconds-of-day)
    and previous close scalar for fast matrix packing.
    """
    if df_prev is None or df_prev.empty or "Volume" not in df_prev.columns:
        return None

    df_prev = _standardize_index_tz(df_prev)
    resolved_date = target_date if target_date is not None else df_prev.index.date.max()
    mask_date = df_prev.index.date == resolved_date
    if not mask_date.any():
        return None
    df_prev = df_prev.loc[mask_date]

    times = df_prev.index.time
    mask_open = times >= MARKET_OPEN
    if not mask_open.any():
        return None

    reg_df = df_prev.loc[mask_open]
    vol_arr = reg_df["Volume"].to_numpy(dtype=float, copy=False)
    # Require at least one non-NaN price or volume value in regular hours
    has_vol = not np.isnan(vol_arr).all()
    has_close = "Close" in reg_df.columns and reg_df["Close"].notna().any()
    if not has_vol and not has_close:
        return None

    reg_times = reg_df.index.time
    secs = np.fromiter((_time_to_sec(t) for t in reg_times), dtype=np.int32, count=len(reg_times))
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
    if "Close" in df_prev.columns:
        df_close = df_prev if df_prev.index.is_monotonic_increasing else df_prev.sort_index(kind="mergesort")
        close_arr = df_close["Close"].to_numpy(dtype=float, copy=False)
        valid_close = close_arr[~np.isnan(close_arr)]
        if len(valid_close) > 0:
            prev_close = float(valid_close[-1])

    return {
        "times_sec": secs_sorted,
        "cumvol": cumvol,
        "prev_close": prev_close,
    }


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

    with _borrow_executor(max_workers) as executor:
        for batch in batches:
            futures = [
                executor.submit(_fetch_one_history, sym, interval, period, start, end)
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
                retry_executor.submit(_fetch_one_history, sym, interval, period_by_sym.get(sym), start, end)
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

        fetched, failed = _fetch_histories_concurrent(
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
            df_prev = fetched.get(sym)
            baseline = _build_prev_day_baseline(df_prev, target_date=prev_day) if df_prev is not None else None
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


def _summarize_today_df(
    df: pd.DataFrame, today: date, cutoff: time
) -> Optional[Tuple[float, Optional[float], Optional[float], Optional[float], Optional[float], Optional[time]]]:
    """Fast NumPy summary of today's regular-hours (vol_today, ltp, open, high, low, last_bar_time)."""
    if df is None or df.empty:
        return None

    df = _standardize_index_tz(df)
    dates = df.index.date
    mask_today = dates == today
    if not mask_today.any():
        return None

    df_today = df if mask_today.all() else df.loc[mask_today]
    times = df_today.index.time
    mask_reg = (times >= MARKET_OPEN) & (times <= cutoff)
    if not mask_reg.any():
        return None

    df_reg = df_today.loc[mask_reg]
    price_cols = [c for c in ("Open", "High", "Low", "Close") if c in df_reg.columns]
    has_price = df_reg[price_cols].notna().any(axis=1) if price_cols else pd.Series(False, index=df_reg.index)
    has_pos_vol = (
        df_reg["Volume"].fillna(0.0) > 0.0
        if "Volume" in df_reg.columns
        else pd.Series(False, index=df_reg.index)
    )
    df_reg = df_reg.loc[has_price | has_pos_vol]
    if df_reg.empty:
        return None

    if not df_reg.index.is_monotonic_increasing:
        df_reg = df_reg.sort_index(kind="mergesort")

    if df_reg.index.has_duplicates:
        agg_map = {
            col: fn
            for col, fn in (
                ("Open", "first"),
                ("High", "max"),
                ("Low", "min"),
                ("Close", "last"),
                ("Volume", "max"),
            )
            if col in df_reg.columns
        }
        if agg_map:
            df_reg = df_reg.groupby(level=0, sort=True).agg(agg_map)
        else:
            df_reg = df_reg[~df_reg.index.duplicated(keep="last")]

    last_bar_time = df_reg.index[-1].time()

    if "Volume" in df_reg.columns:
        vol_arr = df_reg["Volume"].to_numpy(dtype=float, copy=False)
        vol_today = float(np.nansum(vol_arr)) if len(vol_arr) else 0.0
    else:
        vol_today = 0.0

    last_price: Optional[float] = None
    if "Close" in df_reg.columns:
        close_arr = df_reg["Close"].to_numpy(dtype=float, copy=False)
        valid = close_arr[~np.isnan(close_arr)]
        if len(valid):
            last_price = float(valid[-1])

    day_open: Optional[float] = None
    if "Open" in df_reg.columns:
        open_arr = df_reg["Open"].to_numpy(dtype=float, copy=False)
        valid = open_arr[~np.isnan(open_arr)]
        if len(valid):
            day_open = float(valid[0])

    day_high: Optional[float] = None
    if "High" in df_reg.columns:
        high_arr = df_reg["High"].to_numpy(dtype=float, copy=False)
        if not np.isnan(high_arr).all():
            day_high = float(np.nanmax(high_arr))

    day_low: Optional[float] = None
    if "Low" in df_reg.columns:
        low_arr = df_reg["Low"].to_numpy(dtype=float, copy=False)
        if not np.isnan(low_arr).all():
            day_low = float(np.nanmin(low_arr))

    return vol_today, last_price, day_open, day_high, day_low, last_bar_time


def run_scan(
    symbols: List[str],
    min_ratio: float = 1.5,
    min_volume: int = 0,
    interval: str = "1m",
    max_workers: int = 8,
    chunk_size: Optional[int] = None,
) -> pd.DataFrame:
    """
    Run relative volume scan across symbols using:
    - Compact NumPy baseline cache (O(1) vectorized lookup)
    - Single-pass concurrent fetching (coalesces cold baseline + today into 1 HTTP wave via period='5d')
    - Per-ticker bar-aligned comparison (eliminates look-ahead bias on lagging Yahoo feeds)
    - Persistent curl_cffi HTTP/2 session & reusable worker pool
    Pass min_ratio=0.0 and min_volume=0 to retrieve all valid symbols for client-side filtering.
    """
    if not symbols:
        return pd.DataFrame()

    now = datetime.now(IST)
    cutoff = now.time()
    cutoff_sec = _time_to_sec(cutoff)
    today = now.date()
    prev_day = get_previous_trading_day(today)
    cache_key = (prev_day, interval)

    # 1. Load existing baseline store (memory LRU -> disk) under lock
    store = _get_or_load_store(cache_key, prev_day, interval)

    known_set = set(store["symbols"]) | _get_neg_cached(cache_key)
    missing = [sym for sym in symbols if sym not in known_set]
    missing_set = set(missing)

    can_coalesce_5d = interval != "1m" or (today - prev_day).days <= 6

    # 2. Fetch histories in one concurrent wave (coalescing cold prev_day + today via period='5d')
    if missing and not can_coalesce_5d:
        # Rare >6-day holiday gap on 1m bars: fetch prev_day explicitly first
        store = get_previous_day_data(
            symbols, prev_day, interval=interval, chunk_size=chunk_size, max_workers=max_workers
        )
        missing_set = set()

    symbol_periods = (
        (sym, "5d" if (sym in missing_set and can_coalesce_5d) else "1d")
        for sym in symbols
    )
    fetched_dfs, failed_syms = _fetch_histories_concurrent(
        symbol_periods,
        interval=interval,
        max_workers=max_workers,
        chunk_size=chunk_size,
    )

    # 3. Extract & persist any newly fetched previous-day baselines from the 5d responses
    if missing_set:
        _prune_old_cache()
        new_entries: Dict[str, Optional[Dict[str, Any]]] = {}
        neg_syms: List[str] = []
        fallback_missing: List[str] = []

        for sym in missing:
            if sym in failed_syms:
                continue
            df_sym = fetched_dfs.get(sym)
            if df_sym is None or df_sym.empty:
                neg_syms.append(sym)
                continue
            baseline = _build_prev_day_baseline(df_sym, target_date=prev_day)
            if baseline is not None:
                new_entries[sym] = baseline
            else:
                # 5d might not reach prev_day if there were >5 trading sessions or missing bars
                if (df_sym.index.date < prev_day).any():
                    neg_syms.append(sym)
                else:
                    fallback_missing.append(sym)

        if fallback_missing:
            start_str = prev_day.strftime("%Y-%m-%d")
            end_str = (prev_day + timedelta(days=1)).strftime("%Y-%m-%d")
            fb_dfs, fb_failed = _fetch_histories_concurrent(
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
                df_fb = fb_dfs.get(sym)
                baseline = _build_prev_day_baseline(df_fb, target_date=prev_day) if df_fb is not None else None
                if baseline is not None:
                    new_entries[sym] = baseline
                else:
                    neg_syms.append(sym)

        store = _commit_baseline_updates(cache_key, prev_day, interval, new_entries, neg_syms)
    else:
        store = _commit_baseline_updates(cache_key, prev_day, interval, {}, [])

    # 4. Per-ticker bar-aligned baseline lookup (prevents look-ahead bias on lagging feeds)
    times_sec = store["times_sec"]
    if len(times_sec) == 0 or cutoff_sec < int(times_sec[0]):
        return pd.DataFrame()

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

        df_sym = fetched_dfs.get(sym)
        summary = _summarize_today_df(df_sym, today, cutoff) if df_sym is not None else None
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
