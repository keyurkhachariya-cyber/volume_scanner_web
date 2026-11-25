"""
Optimized relative-volume scanner logic using yfinance with:
1. Disk & In-Memory caching (with negative caching) for previous trading day baseline.
2. Batched chunk downloading via yf.download() using a single flat thread pool.
3. Direct previous-close extraction from cached baseline (eliminates fast_info calls).
"""

from datetime import datetime, time, timedelta, date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import pickle
import pandas as pd
import yfinance as yf

from holidays import get_previous_trading_day, IST

MARKET_OPEN = time(9, 15)
CACHE_DIR = Path(__file__).parent / ".cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_RETENTION_DAYS = 7

# In-memory session cache: (date, interval) -> Dict[symbol, Optional[Dict[str, Any]]]
# Each entry stores {"cumvol": pd.Series (indexed by time), "prev_close": Optional[float]} or None.
_PREV_DAY_MEMORY_CACHE: Dict[Tuple[date, str], Dict[str, Optional[Dict[str, Any]]]] = {}


def _prune_old_cache(max_age_days: int = CACHE_RETENTION_DAYS) -> None:
    """Remove cached .pkl files older than max_age_days."""
    cutoff_ts = datetime.now().timestamp() - (max_age_days * 86400)
    for path in CACHE_DIR.glob("*.pkl"):
        try:
            if path.stat().st_mtime < cutoff_ts:
                path.unlink(missing_ok=True)
        except Exception:
            pass


def _build_prev_day_baseline(df_prev: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """
    Pre-compute cumulative regular-hours volume Series (indexed by IST time-of-day)
    and previous close scalar for O(1) .asof() lookups.
    """
    if df_prev is None or df_prev.empty or "Volume" not in df_prev.columns:
        return None

    mask_open = df_prev.index.time >= MARKET_OPEN
    vol_series = df_prev.loc[mask_open, "Volume"].fillna(0.0)
    if vol_series.empty:
        return None

    # Deduplicate/sort by time-of-day so .asof() always sees a strictly monotonic index
    vol_by_time = pd.Series(vol_series.to_numpy(dtype=float), index=vol_series.index.time)
    cumvol = vol_by_time.groupby(level=0).sum().sort_index().cumsum()

    prev_close: Optional[float] = None
    if "Close" in df_prev.columns:
        close_series = df_prev["Close"].dropna()
        if not close_series.empty:
            prev_close = float(close_series.iloc[-1])

    return {"cumvol": cumvol, "prev_close": prev_close}


def _standardize_index_tz(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure DataFrame index is localized/converted to IST."""
    if df.empty:
        return df
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC").tz_convert(IST)
    else:
        df.index = df.index.tz_convert(IST)
    return df


def extract_ticker_df(batch_df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """Extract a single ticker's DataFrame from a batched yf.download result."""
    if batch_df.empty:
        return pd.DataFrame()

    try:
        if isinstance(batch_df.columns, pd.MultiIndex):
            # group_by='ticker': symbol is at level 0
            if symbol in batch_df.columns.levels[0]:
                sub = batch_df[symbol].dropna(how="all")
                return _standardize_index_tz(sub)
            # group_by='column': symbol is at level 1
            elif len(batch_df.columns.levels) > 1 and symbol in batch_df.columns.levels[1]:
                sub = batch_df.xs(symbol, axis=1, level=1).dropna(how="all")
                return _standardize_index_tz(sub)
        elif symbol in batch_df.columns:
            sub = batch_df[[symbol]].dropna(how="all")
            return _standardize_index_tz(sub)
    except Exception:
        pass

    return pd.DataFrame()


def cumulative_volume_until(df: pd.DataFrame, cutoff: time) -> float:
    """Sum volume of bars whose time-of-day <= cutoff (from market open 09:15)."""
    if df.empty or "Volume" not in df.columns:
        return 0.0

    mask_time = (df.index.time >= MARKET_OPEN) & (df.index.time <= cutoff)
    return float(df.loc[mask_time, "Volume"].sum())


def _fetch_batch_chunk(
    tickers: List[str],
    start: str,
    end: str,
    interval: str,
    threads: int = 8,
) -> Tuple[Dict[str, pd.DataFrame], bool]:
    """
    Download a batch chunk of tickers using yfinance's internal thread pool.
    Returns (symbol_dfs, batch_succeeded).
    """
    if not tickers:
        return {}, True

    result: Dict[str, pd.DataFrame] = {}
    try:
        batch_df = yf.download(
            tickers=tickers,
            start=start,
            end=end,
            interval=interval,
            group_by="ticker",
            progress=False,
            threads=max(1, threads),
        )
        if batch_df is None or batch_df.empty:
            return result, False

        for sym in tickers:
            sub = extract_ticker_df(batch_df, sym)
            if not sub.empty:
                result[sym] = sub
        return result, True
    except Exception:
        return result, False


def get_previous_day_data(
    symbols: List[str],
    prev_day: date,
    interval: str = "1m",
    chunk_size: int = 50,
    max_workers: int = 8,
) -> Dict[str, Optional[Dict[str, Any]]]:
    """
    Fetch previous trading day's pre-computed baselines (cumvol Series + prev_close)
    with two-level caching:
    1. In-memory dictionary.
    2. Disk pickle cache in .cache/prev_baseline_{prev_day}_{interval}.pkl.
    Includes negative caching and auto-pruning of cache files older than 7 days.
    """
    cache_key = (prev_day, interval)
    cached_data: Dict[str, Optional[Dict[str, Any]]] = {}

    if cache_key in _PREV_DAY_MEMORY_CACHE:
        cached_data = _PREV_DAY_MEMORY_CACHE[cache_key]

    cache_file = CACHE_DIR / f"prev_baseline_{prev_day.isoformat()}_{interval}.pkl"
    if not cached_data and cache_file.exists():
        try:
            with open(cache_file, "rb") as f:
                cached_data = pickle.load(f)
        except Exception:
            cached_data = {}

    missing = [sym for sym in symbols if sym not in cached_data]

    if missing:
        _prune_old_cache()
        start_str = prev_day.strftime("%Y-%m-%d")
        end_str = (prev_day + timedelta(days=1)).strftime("%Y-%m-%d")
        updated = False

        chunks = [missing[i : i + chunk_size] for i in range(0, len(missing), chunk_size)]
        for chunk in chunks:
            chunk_result, batch_ok = _fetch_batch_chunk(
                chunk, start_str, end_str, interval, threads=max_workers
            )
            for sym, df_prev in chunk_result.items():
                cached_data[sym] = _build_prev_day_baseline(df_prev)
                updated = True

            # Negative-cache missing symbols only when the batch itself succeeded
            if batch_ok:
                for sym in chunk:
                    if sym not in chunk_result:
                        cached_data[sym] = None
                updated = True

        if updated:
            try:
                with open(cache_file, "wb") as f:
                    pickle.dump(cached_data, f, protocol=pickle.HIGHEST_PROTOCOL)
            except Exception:
                pass

    _PREV_DAY_MEMORY_CACHE[cache_key] = cached_data
    return cached_data


def get_today_data(
    symbols: List[str],
    today: date,
    interval: str = "1m",
    chunk_size: int = 50,
    max_workers: int = 8,
) -> Dict[str, pd.DataFrame]:
    """
    Fetch today's bars sequentially across batch chunks using yfinance's flat thread pool.
    Avoids nested thread pools and shared-state contention.
    """
    start_str = today.strftime("%Y-%m-%d")
    end_str = (today + timedelta(days=1)).strftime("%Y-%m-%d")

    chunks = [symbols[i : i + chunk_size] for i in range(0, len(symbols), chunk_size)]
    today_data: Dict[str, pd.DataFrame] = {}

    for chunk in chunks:
        chunk_result, _ = _fetch_batch_chunk(
            chunk, start_str, end_str, interval, threads=max_workers
        )
        today_data.update(chunk_result)

    return today_data


def run_scan(
    symbols: List[str],
    min_ratio: float = 1.5,
    min_volume: int = 0,
    interval: str = "1m",
    max_workers: int = 8,
    chunk_size: int = 50,
) -> pd.DataFrame:
    """
    Run relative volume scan across symbols using:
    - Cached previous-day baseline (0 HTTP requests after 1st fetch)
    - Batched download for today (flat thread pool, no nested pools)
    Pass min_ratio=0.0 and min_volume=0 to retrieve all valid symbols for client-side filtering.
    """
    now = datetime.now(IST)
    cutoff = now.time()
    today = now.date()
    prev_day = get_previous_trading_day(today)

    # 1. Fetch previous day data (cached in-memory / on-disk)
    prev_data = get_previous_day_data(
        symbols, prev_day, interval=interval, chunk_size=chunk_size, max_workers=max_workers
    )

    # 2. Fetch today's data in batched chunks
    today_data = get_today_data(
        symbols,
        today=today,
        interval=interval,
        chunk_size=chunk_size,
        max_workers=max_workers,
    )

    results = []

    # 3. Process metrics in-memory via O(1) .asof() lookup on pre-computed cumsum
    for sym in symbols:
        df_today = today_data.get(sym)
        baseline = prev_data.get(sym)

        if df_today is None or df_today.empty or not baseline:
            continue

        cumvol = baseline["cumvol"]
        if cumvol.empty:
            continue

        raw_prev_vol = cumvol.asof(cutoff)
        if pd.isna(raw_prev_vol) or raw_prev_vol <= 0:
            continue

        vol_prev = float(raw_prev_vol)
        vol_today = cumulative_volume_until(df_today, cutoff)
        ratio = vol_today / vol_prev

        if ratio < min_ratio or vol_today < min_volume:
            continue

        # Extract latest price, intraday OHLC metrics, and previous-day metrics
        last_price = None
        day_open = None
        day_high = None
        day_low = None
        pct_change = None
        range_pct = None

        df_reg = df_today.loc[
            (df_today.index.time >= MARKET_OPEN) & (df_today.index.time <= cutoff)
        ]
        price_df = df_reg if not df_reg.empty else df_today

        if "Close" in price_df.columns:
            close_series = price_df["Close"].dropna()
            if not close_series.empty:
                last_price = float(close_series.iloc[-1])

        if "Open" in price_df.columns:
            open_series = price_df["Open"].dropna()
            if not open_series.empty:
                day_open = float(open_series.iloc[0])

        if "High" in price_df.columns:
            high_series = price_df["High"].dropna()
            if not high_series.empty:
                day_high = float(high_series.max())

        if "Low" in price_df.columns:
            low_series = price_df["Low"].dropna()
            if not low_series.empty:
                day_low = float(low_series.min())

        if day_high is not None and day_low is not None and day_low > 0:
            range_pct = ((day_high - day_low) / day_low) * 100

        prev_close = baseline.get("prev_close")
        gap_pct = None
        if day_open is not None and prev_close and prev_close > 0:
            gap_pct = ((day_open - prev_close) / prev_close) * 100
        if last_price and prev_close and prev_close > 0:
            pct_change = ((last_price - prev_close) / prev_close) * 100

        prev_full_vol = float(cumvol.iloc[-1])
        vol_diff = vol_today - vol_prev
        turnover_cr = ((last_price * vol_today) / 1e7) if last_price is not None else None

        results.append(
            {
                "Symbol": sym.replace(".NS", ""),
                "LTP": round(last_price, 2) if last_price is not None else None,
                "Vol_Today": int(vol_today),
                "Vol_Prev_Same_Time": int(vol_prev),
                "Ratio": round(ratio, 2),
                "Pct_Change": round(pct_change, 2) if pct_change is not None else None,
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

    df = pd.DataFrame(results)
    df = df.sort_values("Ratio", ascending=False).reset_index(drop=True)
    return df
