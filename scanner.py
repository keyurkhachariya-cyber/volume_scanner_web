"""
Core intraday relative-volume scanner:
- Summarizes today's regular-hours volume, OHLC, VWAP, and 15m volume burst from NumPy arrays.
- Orchestrates concurrent baseline + today data fetching via yahoo_client and baseline_cache.
- Aligns per-ticker lookup to previous trading day's volume till the same bar time and enriches with NSE OI.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd

import baseline_cache
from baseline_cache import BASELINE_FORMAT_VERSION, get_previous_day_data, prune_cache
from holidays import get_previous_trading_day
from oi import classify_oi_buildup, classify_trade_signal, get_oi_snapshot
from utils import IST, ist_epoch_sec as _ist_epoch_sec, time_to_sec as _time_to_sec
import yahoo_client

_MARKET_OPEN_SEC = 9 * 3600 + 15 * 60


def _compute_vwap(
    vol_dedup: Optional[np.ndarray],
    vol_today: float,
    high_dedup: Optional[np.ndarray],
    low_dedup: Optional[np.ndarray],
    close_dedup: Optional[np.ndarray],
) -> Optional[float]:
    """Compute regular-hours intraday VWAP using typical price fallback to close."""
    if vol_dedup is None or vol_today <= 0.0 or close_dedup is None:
        return None
    if high_dedup is not None and low_dedup is not None:
        tp = (high_dedup + low_dedup + close_dedup) / 3.0
        missing_tp = np.isnan(tp)
        if missing_tp.any():
            tp = np.where(missing_tp, close_dedup, tp)
    else:
        tp = close_dedup

    valid_vwap = (~np.isnan(tp)) & (vol_dedup > 0.0)
    if not valid_vwap.any():
        return None
    v_sub = vol_dedup[valid_vwap]
    v_sum = float(np.sum(v_sub, dtype=np.float64))
    if v_sum <= 0.0:
        return None
    return float(np.sum(tp[valid_vwap] * v_sub, dtype=np.float64) / v_sum)


def _compute_vol_burst_15m(
    vol_dedup: Optional[np.ndarray],
    vol_today: float,
    bar_interval_sec: int,
) -> Optional[float]:
    """Compare mean volume of trailing active 15m bars against session active-bar mean (immune to NaN gaps)."""
    if vol_dedup is None or vol_today <= 0.0:
        return None
    step_sec = max(60, int(bar_interval_sec))
    recent_bars = max(1, 900 // step_sec)
    if len(vol_dedup) <= recent_bars:
        return None
    recent_avg = float(np.mean(vol_dedup[-recent_bars:], dtype=np.float64))
    session_avg = vol_today / float(len(vol_dedup))
    return (recent_avg / session_avg) if session_avg > 0.0 else None


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
    bar_interval_sec: int = 60,
) -> Optional[
    Tuple[
        float,
        Optional[float],
        Optional[float],
        Optional[float],
        Optional[float],
        Optional[time],
        Optional[float],
        Optional[float],
    ]
]:
    """Fast NumPy summary of today's regular-hours (vol_today, ltp, open, high, low, last_bar_time, vwap, vol_burst_15m)."""
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

    vol_dedup: Optional[np.ndarray] = None
    h_dedup = high_arr
    l_dedup = low_arr
    c_dedup = close_arr
    if vol_arr is not None and len(vol_arr):
        vol_clean = np.nan_to_num(vol_arr, nan=0.0)
        if len(t_reg) > 1 and np.any(t_reg[1:] == t_reg[:-1]):
            _, idx_start = np.unique(t_reg, return_index=True)
            vol_dedup = np.maximum.reduceat(vol_clean, idx_start)
            h_dedup = high_arr[idx_start] if high_arr is not None else None
            l_dedup = low_arr[idx_start] if low_arr is not None else None
            c_dedup = close_arr[idx_start] if close_arr is not None else None
        else:
            vol_dedup = vol_clean
        vol_today = float(np.sum(vol_dedup, dtype=np.float64))
    else:
        vol_today = 0.0

    last_price = float(close_arr[~np.isnan(close_arr)][-1]) if (close_arr is not None and (~np.isnan(close_arr)).any()) else None
    day_open = float(open_arr[~np.isnan(open_arr)][0]) if (open_arr is not None and (~np.isnan(open_arr)).any()) else None
    day_high = float(np.nanmax(high_arr)) if (high_arr is not None and not np.isnan(high_arr).all()) else None
    day_low = float(np.nanmin(low_arr)) if (low_arr is not None and not np.isnan(low_arr).all()) else None

    vwap = _compute_vwap(vol_dedup, vol_today, h_dedup, l_dedup, c_dedup)
    vol_burst_15m = _compute_vol_burst_15m(vol_dedup, vol_today, bar_interval_sec)

    return vol_today, last_price, day_open, day_high, day_low, last_bar_time, vwap, vol_burst_15m


def _prepare_fetch_plan(
    symbols: List[str],
    today: date,
    prev_day: date,
    interval: str,
    max_workers: int,
    chunk_size: Optional[int],
) -> Tuple[
    Dict[str, Any],
    List[str],
    Set[str],
    List[Tuple[str, Optional[str]]],
    Dict[str, Tuple[Optional[str], Optional[str]]],
]:
    """Load cached baselines and build coalesced fetch tasks for today (+ cold prev_day where supported)."""
    cache_key = (prev_day, interval)
    store = baseline_cache._get_or_load_store(cache_key, prev_day, interval)
    neg_set = baseline_cache._get_neg_cached(cache_key)
    known_set = set(store["symbols"]) | neg_set
    missing = [sym for sym in symbols if sym not in known_set]
    missing_set = set(missing)

    gap_days = (today - prev_day).days
    can_coalesce_range = 0 < gap_days <= (6 if interval == "1m" else 55)

    if missing and not can_coalesce_range:
        store = baseline_cache.get_previous_day_data(
            symbols, prev_day, interval=interval, chunk_size=chunk_size, max_workers=max_workers
        )
        missing_set = set()
        neg_set = baseline_cache._get_neg_cached(cache_key)

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

    return store, missing, missing_set, symbol_periods, symbol_ranges


def _extract_and_commit_baselines(
    missing: List[str],
    fetched_arrays: Dict[str, Dict[str, Optional[np.ndarray]]],
    failed_syms: Set[str],
    prev_day: date,
    interval: str,
    max_workers: int,
    chunk_size: Optional[int],
) -> Dict[str, Any]:
    """Extract prev_day baselines from coalesced fetches (with bounded fallback) and commit to cache."""
    baseline_cache._prune_old_cache()
    cache_key = (prev_day, interval)
    new_entries: Dict[str, Optional[Dict[str, Any]]] = {}
    neg_syms: List[str] = []
    fallback_missing: List[str] = []
    prev_day_start_sec = _ist_epoch_sec(prev_day, time(0, 0))
    nat_int64 = yahoo_client._NAT_INT64

    for sym in missing:
        if sym in failed_syms:
            continue
        bundle = fetched_arrays.get(sym)
        if bundle is None or len(bundle["ts_sec"]) == 0:
            neg_syms.append(sym)
            continue
        baseline = baseline_cache._build_prev_day_baseline_from_arrays(
            bundle["ts_sec"], bundle["close"], bundle["volume"], target_date=prev_day
        )
        if baseline is not None:
            new_entries[sym] = baseline
        else:
            ts_sym = bundle["ts_sec"]
            valid_ts = ts_sym[ts_sym != nat_int64]
            if len(valid_ts) > 0 and (valid_ts < prev_day_start_sec).any():
                neg_syms.append(sym)
            else:
                fallback_missing.append(sym)

    if fallback_missing:
        start_str = prev_day.strftime("%Y-%m-%d")
        end_str = (prev_day + timedelta(days=1)).strftime("%Y-%m-%d")
        fb_arrays, fb_failed = yahoo_client._fetch_market_data(
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
                baseline_cache._build_prev_day_baseline_from_arrays(
                    fb_bundle["ts_sec"], fb_bundle["close"], fb_bundle["volume"], target_date=prev_day
                )
                if fb_bundle is not None
                else None
            )
            if baseline is not None:
                new_entries[sym] = baseline
            else:
                neg_syms.append(sym)

    return baseline_cache._commit_baseline_updates(cache_key, prev_day, interval, new_entries, neg_syms)


def _build_scan_row(
    sym: str,
    summary: Tuple[Any, ...],
    cutoff: time,
    times_sec: np.ndarray,
    cumvol_mat: np.ndarray,
    col_idx: int,
    raw_prev_close: float,
    prev_full_vol: float,
    min_ratio: float,
    min_volume: int,
    row_idx_cache: Dict[int, int],
    include_oi: bool,
    oi_by_symbol: Dict[str, Dict[str, Optional[float]]],
) -> Optional[Dict[str, Any]]:
    """Build one result dict for a ticker if it meets volume/ratio gates."""
    if len(summary) == 6:
        vol_today, last_price, day_open, day_high, day_low, last_bar_time = summary
        vwap = None
        vol_burst_15m = None
    else:
        vol_today, last_price, day_open, day_high, day_low, last_bar_time, vwap, vol_burst_15m = summary

    if last_bar_time is None:
        return None

    aligned_time = last_bar_time if last_bar_time <= cutoff else cutoff
    aligned_sec = _time_to_sec(aligned_time)
    t_idx = row_idx_cache.get(aligned_sec)
    if t_idx is None:
        t_idx = int(np.searchsorted(times_sec, aligned_sec, side="right")) - 1
        row_idx_cache[aligned_sec] = t_idx
    if t_idx < 0:
        return None

    vol_prev = float(cumvol_mat[t_idx, col_idx])
    if np.isnan(vol_prev) or vol_prev <= 0.0:
        return None

    ratio = vol_today / vol_prev
    if ratio < min_ratio or vol_today < min_volume:
        return None

    prev_close = None if np.isnan(raw_prev_close) else raw_prev_close
    range_pct = ((day_high - day_low) / day_low) * 100 if (day_high is not None and day_low is not None and day_low > 0) else None

    gap_pct = None
    pct_change = None
    if prev_close is not None and prev_close > 0:
        if day_open is not None:
            gap_pct = ((day_open - prev_close) / prev_close) * 100
        if last_price is not None:
            pct_change = ((last_price - prev_close) / prev_close) * 100

    from_open_pct = ((last_price - day_open) / day_open) * 100 if (last_price is not None and day_open is not None and day_open > 0) else None
    vwap_dist_pct = ((last_price - vwap) / vwap) * 100 if (last_price is not None and vwap is not None and vwap > 0) else None
    range_pos_pct = min(100.0, max(0.0, ((last_price - day_low) / (day_high - day_low)) * 100)) if (
        last_price is not None and day_high is not None and day_low is not None and day_high > day_low
    ) else None

    sym_clean = sym.removesuffix(".NS")
    row_data: Dict[str, Any] = {
        "Symbol": sym_clean,
        "LTP": round(last_price, 2) if last_price is not None else None,
        "Vol_Today": int(vol_today),
        "Vol_Prev_Same_Time": int(vol_prev),
        "Ratio": round(ratio, 2),
        "Vol_Burst_15m": round(vol_burst_15m, 2) if vol_burst_15m is not None else None,
        "Pct_Change": round(pct_change, 2) if pct_change is not None else None,
        "From_Open_Pct": round(from_open_pct, 2) if from_open_pct is not None else None,
        "VWAP_Dist_Pct": round(vwap_dist_pct, 2) if vwap_dist_pct is not None else None,
        "Range_Pos_Pct": round(range_pos_pct, 1) if range_pos_pct is not None else None,
        "Last_Bar_Time": aligned_time.strftime("%H:%M"),
        "Gap_Pct": round(gap_pct, 2) if gap_pct is not None else None,
        "VWAP": round(vwap, 2) if vwap is not None else None,
        "Prev_Close": round(prev_close, 2) if prev_close is not None else None,
        "Day_Open": round(day_open, 2) if day_open is not None else None,
        "Day_High": round(day_high, 2) if day_high is not None else None,
        "Day_Low": round(day_low, 2) if day_low is not None else None,
        "Range_Pct": round(range_pct, 2) if range_pct is not None else None,
        "Vol_Diff": int(vol_today - vol_prev),
        "Prev_Full_Vol": int(prev_full_vol),
        "Turnover_Cr": round((last_price * vol_today) / 1e7, 2) if last_price is not None else None,
    }
    if include_oi:
        oi_rec = oi_by_symbol.get(sym_clean) or {}
        oi_val = oi_rec.get("oi")
        oi_prev = oi_rec.get("oi_prev")
        oi_chg = oi_rec.get("oi_chg")
        oi_chg_pct = oi_rec.get("oi_chg_pct")
        oi_chg_rounded = int(round(oi_chg)) if oi_chg is not None else None
        oi_chg_pct_rounded = round(oi_chg_pct, 2) if oi_chg_pct is not None else None
        row_data.update(
            {
                "OI": int(round(oi_val)) if oi_val is not None else None,
                "OI_Prev": int(round(oi_prev)) if oi_prev is not None else None,
                "OI_Chg": oi_chg_rounded,
                "OI_Chg_Pct": oi_chg_pct_rounded,
                "OI_Buildup": classify_oi_buildup(row_data["Pct_Change"], oi_chg_rounded),
                "Signal": classify_trade_signal(row_data["Ratio"], oi_chg_pct_rounded, row_data["Pct_Change"]),
            }
        )
    return row_data


def run_scan(
    symbols: List[str],
    min_ratio: float = 1.5,
    min_volume: int = 0,
    interval: str = "1m",
    max_workers: int = 8,
    chunk_size: Optional[int] = None,
    prev_day: Optional[date] = None,
    include_oi: bool = False,
) -> pd.DataFrame:
    """
    Run relative volume scan across symbols with bar-aligned baseline comparisons and optional NSE OI.
    Pass min_ratio=0.0 and min_volume=0 to retrieve all valid symbols for client-side filtering.
    """
    if not symbols:
        return pd.DataFrame()

    now = datetime.now(IST)
    cutoff = now.time()
    cutoff_sec = _time_to_sec(cutoff)
    today = now.date()

    oi_executor: Optional[ThreadPoolExecutor] = None
    oi_future = None
    if include_oi:
        oi_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="nse_oi_fetch")
        oi_future = oi_executor.submit(get_oi_snapshot)

    try:
        if prev_day is None:
            prev_day = get_previous_trading_day(today)
        store, missing, missing_set, symbol_periods, symbol_ranges = _prepare_fetch_plan(
            symbols, today, prev_day, interval, max_workers, chunk_size
        )
        fetched_arrays, failed_syms = yahoo_client._fetch_market_data(
            symbol_periods,
            interval=interval,
            max_workers=max_workers,
            chunk_size=chunk_size,
            symbol_ranges=symbol_ranges if symbol_ranges else None,
        )
    except Exception:
        if oi_executor is not None:
            oi_executor.shutdown(wait=False)
        raise

    if missing_set:
        store = _extract_and_commit_baselines(
            missing, fetched_arrays, failed_syms, prev_day, interval, max_workers, chunk_size
        )

    oi_snapshot: Dict[str, Any] = {}
    if oi_future is not None:
        try:
            oi_snapshot = oi_future.result() or {}
        except Exception as exc:
            oi_snapshot = {"by_symbol": {}, "error": str(exc), "cooldown_seconds": 0.0}
        finally:
            if oi_executor is not None:
                oi_executor.shutdown(wait=False)
    oi_by_symbol: Dict[str, Dict[str, Optional[float]]] = oi_snapshot.get("by_symbol") or {}

    times_sec = store["times_sec"]
    if len(times_sec) == 0 or cutoff_sec < int(times_sec[0]):
        empty_df = pd.DataFrame()
        if include_oi:
            empty_df.attrs["oi_meta"] = {k: v for k, v in oi_snapshot.items() if k != "by_symbol"}
        return empty_df

    today_day_start_sec = _ist_epoch_sec(today, time(0, 0))
    today_cutoff_bound_sec = today_day_start_sec + cutoff_sec
    bar_interval_sec = 300 if interval == "5m" else 60
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
                bar_interval_sec,
            )
            if bundle is not None
            else None
        )
        if summary is None:
            continue

        row = _build_scan_row(
            sym=sym,
            summary=summary,
            cutoff=cutoff,
            times_sec=times_sec,
            cumvol_mat=cumvol_mat,
            col_idx=col_idx,
            raw_prev_close=float(prev_close_arr[col_idx]),
            prev_full_vol=float(prev_full_arr[col_idx]),
            min_ratio=min_ratio,
            min_volume=min_volume,
            row_idx_cache=row_idx_cache,
            include_oi=include_oi,
            oi_by_symbol=oi_by_symbol,
        )
        if row is not None:
            results.append(row)

    if not results:
        empty_df = pd.DataFrame()
        if include_oi:
            empty_df.attrs["oi_meta"] = {k: v for k, v in oi_snapshot.items() if k != "by_symbol"}
        return empty_df

    df_out = pd.DataFrame(results)
    df_out = df_out.sort_values("Ratio", ascending=False).reset_index(drop=True)
    if include_oi:
        df_out.attrs["oi_meta"] = {k: v for k, v in oi_snapshot.items() if k != "by_symbol"}
    return df_out
