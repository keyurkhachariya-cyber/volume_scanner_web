"""
Optimized relative-volume scanner logic using yfinance with:
1. Disk & In-Memory caching for previous trading day baseline (cuts 50% API calls).
2. Batched chunk downloading via yf.download() (replaces 150+ individual HTTP connections).
3. Direct previous-close extraction from cached baseline (eliminates 150 fast_info calls).
"""

from datetime import datetime, time, timedelta, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import pickle
import pandas as pd
import yfinance as yf
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed

from holidays import get_previous_trading_day, IST

MARKET_OPEN = time(9, 15)
CACHE_DIR = Path(__file__).parent / ".cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# In-memory session cache: (date, interval) -> Dict[symbol, pd.DataFrame]
_PREV_DAY_MEMORY_CACHE: Dict[Tuple[date, str], Dict[str, pd.DataFrame]] = {}


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
) -> Dict[str, pd.DataFrame]:
    """Download a batch chunk of tickers and return symbol-keyed DataFrames."""
    if not tickers:
        return {}

    result = {}
    try:
        batch_df = yf.download(
            tickers=tickers,
            start=start,
            end=end,
            interval=interval,
            group_by="ticker",
            progress=False,
            threads=True,
        )
        for sym in tickers:
            sub = extract_ticker_df(batch_df, sym)
            if not sub.empty:
                result[sym] = sub
    except Exception:
        pass

    return result


def get_previous_day_data(
    symbols: List[str],
    prev_day: date,
    interval: str = "1m",
    chunk_size: int = 50,
) -> Dict[str, pd.DataFrame]:
    """
    Fetch previous trading day's bars with two-level caching:
    1. In-memory dictionary.
    2. Disk pickle cache in .cache/prev_{prev_day}_{interval}.pkl.
    Only missing symbols are fetched via batched yf.download().
    """
    cache_key = (prev_day, interval)
    cached_data: Dict[str, pd.DataFrame] = {}

    # Check in-memory cache
    if cache_key in _PREV_DAY_MEMORY_CACHE:
        cached_data = _PREV_DAY_MEMORY_CACHE[cache_key]

    # Check disk cache if in-memory is empty
    cache_file = CACHE_DIR / f"prev_{prev_day.isoformat()}_{interval}.pkl"
    if not cached_data and cache_file.exists():
        try:
            with open(cache_file, "rb") as f:
                cached_data = pickle.load(f)
        except Exception:
            cached_data = {}

    # Determine missing symbols
    missing = [sym for sym in symbols if sym not in cached_data]

    if missing:
        start_str = prev_day.strftime("%Y-%m-%d")
        end_str = (prev_day + timedelta(days=1)).strftime("%Y-%m-%d")

        chunks = [missing[i : i + chunk_size] for i in range(0, len(missing), chunk_size)]
        with ThreadPoolExecutor(max_workers=min(len(chunks), 4)) as executor:
            futures = [
                executor.submit(_fetch_batch_chunk, chunk, start_str, end_str, interval)
                for chunk in chunks
            ]
            for future in as_completed(futures):
                chunk_result = future.result()
                cached_data.update(chunk_result)

        # Save to disk cache
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
    max_workers: int = 4,
) -> Dict[str, pd.DataFrame]:
    """
    Fetch today's bars using batched yf.download() across chunks in parallel.
    """
    start_str = today.strftime("%Y-%m-%d")
    end_str = (today + timedelta(days=1)).strftime("%Y-%m-%d")

    chunks = [symbols[i : i + chunk_size] for i in range(0, len(symbols), chunk_size)]
    today_data: Dict[str, pd.DataFrame] = {}

    with ThreadPoolExecutor(max_workers=min(len(chunks), max_workers)) as executor:
        futures = [
            executor.submit(_fetch_batch_chunk, chunk, start_str, end_str, interval)
            for chunk in chunks
        ]
        for future in as_completed(futures):
            chunk_result = future.result()
            today_data.update(chunk_result)

    return today_data


def run_scan(
    symbols: List[str],
    min_ratio: float = 1.5,
    min_volume: int = 0,
    interval: str = "1m",
    max_workers: int = 4,
    chunk_size: int = 50,
) -> pd.DataFrame:
    """
    Run relative volume scan across symbols using:
    - Cached previous-day baseline (0 HTTP requests after 1st fetch)
    - Batched download for today (only a few HTTP requests total)
    """
    now = datetime.now(IST)
    cutoff = now.time()
    today = now.date()
    prev_day = get_previous_trading_day(today)

    # 1. Fetch previous day data (cached in-memory / on-disk)
    prev_data = get_previous_day_data(symbols, prev_day, interval=interval, chunk_size=chunk_size)

    # 2. Fetch today's data in batched chunks
    today_data = get_today_data(
        symbols,
        today=today,
        interval=interval,
        chunk_size=chunk_size,
        max_workers=max_workers,
    )

    results = []

    # 3. Process metrics in-memory
    for sym in symbols:
        df_today = today_data.get(sym)
        df_prev = prev_data.get(sym)

        if df_today is None or df_today.empty:
            continue
        if df_prev is None or df_prev.empty:
            continue

        vol_today = cumulative_volume_until(df_today, cutoff)
        vol_prev = cumulative_volume_until(df_prev, cutoff)

        if vol_prev <= 0:
            continue

        ratio = vol_today / vol_prev

        if ratio < min_ratio or vol_today < min_volume:
            continue

        # Extract latest price and previous close directly from cached bars
        last_price = None
        pct_change = None
        if not df_today.empty and "Close" in df_today.columns:
            last_price = float(df_today["Close"].dropna().iloc[-1])

        if not df_prev.empty and "Close" in df_prev.columns:
            prev_close_series = df_prev["Close"].dropna()
            if not prev_close_series.empty:
                prev_close = float(prev_close_series.iloc[-1])
                if last_price and prev_close > 0:
                    pct_change = ((last_price - prev_close) / prev_close) * 100

        results.append(
            {
                "Symbol": sym.replace(".NS", ""),
                "LTP": round(last_price, 2) if last_price is not None else None,
                "Vol_Today": int(vol_today),
                "Vol_Prev_Same_Time": int(vol_prev),
                "Ratio": round(ratio, 2),
                "Pct_Change": round(pct_change, 2) if pct_change is not None else None,
            }
        )

    if not results:
        return pd.DataFrame()

    df = pd.DataFrame(results)
    df = df.sort_values("Ratio", ascending=False).reset_index(drop=True)
    return df
