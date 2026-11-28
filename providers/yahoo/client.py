"""
Yahoo Finance market data fetching client:
1. Multiplexed curl_cffi AsyncSession HTTP/2 Chart API fetcher directly to NumPy array bundles.
2. Automatic yfinance thread-pool fallback with thread-local persistent sessions and ref-counted pools.
"""

import asyncio
import atexit
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import date, time
import sys
import threading
import time as pytime
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import quote as _url_quote

import numpy as np
import pandas as pd
import yfinance as yf
from yfinance.data import YfData

try:
    from yfinance.multi import new_session as _yf_new_session
except ImportError:  # pragma: no cover - fallback for older yfinance versions
    _yf_new_session = None

from core.utils import IST, decode_json_response, ist_epoch_sec

_NAT_INT64 = np.iinfo(np.int64).min
_CHART_HOSTS = ("query1.finance.yahoo.com", "query2.finance.yahoo.com")
_MAX_POOLS = 4

# Persistent thread-local HTTP sessions & reusable worker pools (kept warm across scans)
_SESSION_LOCK = threading.Lock()
_YF_PRIMING_DONE = False
_THREAD_LOCAL = threading.local()

_EXECUTOR_LOCK = threading.Lock()
_EXECUTORS: "OrderedDict[int, Dict[str, Any]]" = OrderedDict()

_ASYNC_LOOP_LOCK = threading.Lock()
_ASYNC_LOOP: Optional[asyncio.AbstractEventLoop] = None
_ASYNC_THREAD: Optional[threading.Thread] = None
_ASYNC_SESSION: Any = None
_ASYNC_CRUMB: Optional[str] = None
_ASYNC_CRUMB_LOCK: Optional[asyncio.Lock] = None


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


def _index_epoch_seconds(index: pd.DatetimeIndex) -> np.ndarray:
    """Convert DatetimeIndex to int64 UTC epoch seconds regardless of ns/us/ms/s resolution."""
    idx = index if index.tz is not None else index.tz_localize("UTC")
    as_unit = getattr(idx, "as_unit", None)
    if as_unit is not None:
        return as_unit("s").asi8
    return (idx.asi8 // 1_000_000_000).astype(np.int64, copy=False)


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

    range_epoch_cache: Dict[Tuple[str, str], Tuple[int, int]] = {}

    def _epoch_bounds(s_str: str, e_str: str) -> Tuple[int, int]:
        key = (s_str, e_str)
        cached = range_epoch_cache.get(key)
        if cached is not None:
            return cached
        bounds = (
            ist_epoch_sec(date.fromisoformat(s_str), time(0, 0)),
            ist_epoch_sec(date.fromisoformat(e_str), time(0, 0)),
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

    saw_429 = False
    retry_after_sec = 1.5
    host_cooldown_until = 0.0

    async def _fetch_one(
        sym: str,
        period: Optional[str],
        sem: asyncio.Semaphore,
        host_idx: int = 0,
        allow_auth_retry: bool = True,
    ) -> Tuple[str, Optional[Dict[str, Optional[np.ndarray]]], bool]:
        nonlocal saw_429, retry_after_sec, host_cooldown_until
        host = _CHART_HOSTS[host_idx % len(_CHART_HOSTS)]
        url = f"https://{host}/v8/finance/chart/{_url_quote(sym, safe='')}"
        async with sem:
            try:
                now_mono = pytime.monotonic()
                if host_cooldown_until > now_mono:
                    await asyncio.sleep(min(2.0, host_cooldown_until - now_mono))
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
                        _, ok = _parse_yahoo_chart_payload(decode_json_response(resp))
                    except Exception:
                        ok = False
                    return sym, None, ok
                if resp.status_code == 429:
                    saw_429 = True
                    hdrs = getattr(resp, "headers", None)
                    raw_ra = hdrs.get("Retry-After") if hdrs is not None else None
                    ra_val = 1.5
                    if raw_ra is not None:
                        try:
                            ra_val = min(15.0, max(1.0, float(str(raw_ra).strip())))
                        except (TypeError, ValueError):
                            ra_val = 1.5
                    retry_after_sec = max(retry_after_sec, ra_val)
                    host_cooldown_until = max(host_cooldown_until, pytime.monotonic() + ra_val)
                    return sym, None, False
                if resp.status_code != 200:
                    return sym, None, False
                bundle, ok = _parse_yahoo_chart_payload(decode_json_response(resp))
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
            backoff = max(retry_after_sec, host_cooldown_until - pytime.monotonic()) if saw_429 else 0.5
            await asyncio.sleep(backoff)
            retry_cap = 2 if saw_429 else 4
            retry_sem = asyncio.Semaphore(min(retry_cap, max(1, int(max_concurrency) // 4)))
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


def _tasks_order(tasks: List[Tuple[str, Optional[str]]], subset: Set[str]) -> List[str]:
    return [sym for sym, _ in tasks if sym in subset]


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
        async_concurrency = min(18, max(8, int(max_workers) * 3))
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
