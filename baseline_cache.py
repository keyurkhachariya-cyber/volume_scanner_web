"""
Previous-day cumulative volume baseline store & disk/LRU memory cache.
Stores per-symbol regular-hours cumulative volume in a compact v2 2D NumPy matrix
(.cache/prev_baseline_<date>_<interval>.pkl) for O(1) vectorized lookup.
"""

from collections import OrderedDict
from datetime import date, datetime, time, timedelta
import os
from pathlib import Path
import pickle
import tempfile
import threading
import time as pytime
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import numpy as np
import pandas as pd

from utils import IST, ist_epoch_sec, time_to_sec
import yahoo_client

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


def _prune_old_cache(max_age_days: int = CACHE_RETENTION_DAYS) -> None:
    """Remove stale .pkl files and legacy prev_*.pkl cache artifacts."""
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

    day_start_sec = ist_epoch_sec(resolved_date, time(0, 0))
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


def _upgrade_v1_baseline(raw_v1: Dict[str, Any], prev_day: date, interval: str) -> Dict[str, Any]:
    """Convert legacy v1 dict ({sym: {'cumvol': pd.Series, 'prev_close': float}}) into v2 store."""
    entries: Dict[str, Optional[Dict[str, Any]]] = {}
    for sym, val in raw_v1.items():
        if val is None:
            continue
        series = val.get("cumvol")
        if series is None or series.empty:
            continue
        secs = np.fromiter((time_to_sec(t) for t in series.index), dtype=np.int32, count=len(series))
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


def _get_or_load_store(cache_key: Tuple[date, str], prev_day: date, interval: str) -> Dict[str, Any]:
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

        fetched, failed = yahoo_client._fetch_market_data(
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
