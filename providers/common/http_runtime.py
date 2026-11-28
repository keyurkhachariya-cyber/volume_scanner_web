"""
Shared persistent HTTP/2 AsyncSession runtime and sliding-window rate limiter for broker providers.
Mirrors providers/yahoo/client.py's persistent event-loop + connection-reuse pattern to eliminate per-scan TLS setup.
"""

from __future__ import annotations

import asyncio
import atexit
from collections import deque
import threading
import time as pytime
from typing import Any, Callable, Deque, Dict, List, Optional, Set, Tuple

from core.utils import decode_json_response
from providers.base import ArrayBundle
import providers.yahoo.client as yahoo_client

_STATE_LOCK = threading.Lock()
_BROKER_ASYNC_SESSION: Any = None
_RATE_LIMITERS: Dict[str, Tuple[asyncio.AbstractEventLoop, "AsyncSlidingRateLimiter"]] = {}


def _parse_retry_after(resp: Any, default_sec: float = 1.25, max_sec: float = 15.0) -> float:
    """Safely parse numeric Retry-After header seconds from an HTTP response."""
    headers = getattr(resp, "headers", None)
    raw = headers.get("Retry-After") if headers is not None else None
    if raw is not None:
        try:
            parsed = float(str(raw).strip())
            if parsed > 0.0:
                return min(max_sec, max(0.5, parsed))
        except (TypeError, ValueError):
            pass
    return default_sec


class AsyncSlidingRateLimiter:
    """Thread/loop-local sliding-window rate limiter with shared 429 cooldown across callers."""

    def __init__(self, max_per_sec: int, max_per_min: int, max_per_30min: int = 0) -> None:
        self._max_per_sec = max(0, int(max_per_sec))
        self._max_per_min = max(0, int(max_per_min))
        self._max_per_30min = max(0, int(max_per_30min))
        self._sec_q: Deque[float] = deque()
        self._min_q: Deque[float] = deque()
        self._long_q: Deque[float] = deque()
        self._cooldown_until: float = 0.0
        self._lock = asyncio.Lock()

    async def penalize(self, delay_sec: float) -> float:
        """Advance shared cooldown window after HTTP 429 so sibling tasks pause before dispatching."""
        delay = max(0.25, float(delay_sec))
        async with self._lock:
            now = pytime.monotonic()
            target = now + delay
            if target > self._cooldown_until:
                self._cooldown_until = target
            return self._cooldown_until - now

    def remaining_cooldown(self) -> float:
        return max(0.0, self._cooldown_until - pytime.monotonic())

    async def acquire(self) -> None:
        if (
            self._max_per_sec <= 0
            and self._max_per_min <= 0
            and self._max_per_30min <= 0
            and self._cooldown_until <= 0.0
        ):
            return
        while True:
            wait_sec = 0.0
            async with self._lock:
                now = pytime.monotonic()
                if self._cooldown_until > now:
                    wait_sec = max(wait_sec, self._cooldown_until - now)

                sec_cutoff = now - 1.0
                while self._sec_q and self._sec_q[0] <= sec_cutoff:
                    self._sec_q.popleft()
                min_cutoff = now - 60.0
                while self._min_q and self._min_q[0] <= min_cutoff:
                    self._min_q.popleft()
                long_cutoff = now - 1800.0
                while self._long_q and self._long_q[0] <= long_cutoff:
                    self._long_q.popleft()

                if self._max_per_sec > 0 and len(self._sec_q) >= self._max_per_sec:
                    wait_sec = max(wait_sec, (self._sec_q[0] + 1.01) - now)
                if self._max_per_min > 0 and len(self._min_q) >= self._max_per_min:
                    wait_sec = max(wait_sec, (self._min_q[0] + 60.02) - now)
                if self._max_per_30min > 0 and len(self._long_q) >= self._max_per_30min:
                    wait_sec = max(wait_sec, (self._long_q[0] + 1800.05) - now)

                if wait_sec <= 0.0:
                    if self._max_per_sec > 0:
                        self._sec_q.append(now)
                    if self._max_per_min > 0:
                        self._min_q.append(now)
                    if self._max_per_30min > 0:
                        self._long_q.append(now)
                    return
            await asyncio.sleep(min(wait_sec, 1.0))


def _shutdown_broker_session() -> None:
    """Close shared persistent broker AsyncSession cleanly before yahoo_client shuts down _ASYNC_LOOP."""
    global _BROKER_ASYNC_SESSION
    with _STATE_LOCK:
        sess = _BROKER_ASYNC_SESSION
        _BROKER_ASYNC_SESSION = None
        _RATE_LIMITERS.clear()
    loop = yahoo_client._ASYNC_LOOP
    if sess is None or loop is None or loop.is_closed():
        return
    try:
        fut = asyncio.run_coroutine_threadsafe(sess.close(), loop)
        fut.result(timeout=2.0)
    except Exception:
        pass


atexit.register(_shutdown_broker_session)


def _get_limiter(
    limiter_key: str,
    running_loop: Optional[asyncio.AbstractEventLoop],
    persistent: bool,
    max_per_sec: int,
    max_per_min: int,
    max_per_30min: int = 0,
) -> AsyncSlidingRateLimiter:
    if not persistent or running_loop is None:
        return AsyncSlidingRateLimiter(max_per_sec, max_per_min, max_per_30min=max_per_30min)
    with _STATE_LOCK:
        cached = _RATE_LIMITERS.get(limiter_key)
        if cached is not None and cached[0] is running_loop:
            limiter = cached[1]
            if (
                limiter._max_per_sec == max_per_sec
                and limiter._max_per_min == max_per_min
                and limiter._max_per_30min == max_per_30min
            ):
                return limiter
        limiter = AsyncSlidingRateLimiter(max_per_sec, max_per_min, max_per_30min=max_per_30min)
        _RATE_LIMITERS[limiter_key] = (running_loop, limiter)
        return limiter


async def _acquire_session() -> Tuple[Any, bool]:
    """Return (session, should_close_ephemeral) reusing persistent HTTP/2 session on yahoo_client loop."""
    global _BROKER_ASYNC_SESSION
    from curl_cffi.requests import AsyncSession

    try:
        running_loop = asyncio.get_running_loop()
    except RuntimeError:
        running_loop = None

    persistent = yahoo_client._ASYNC_LOOP is not None and running_loop is yahoo_client._ASYNC_LOOP
    if persistent:
        with _STATE_LOCK:
            if _BROKER_ASYNC_SESSION is None:
                _BROKER_ASYNC_SESSION = AsyncSession(impersonate="chrome")
            return _BROKER_ASYNC_SESSION, False

    sess = AsyncSession(impersonate="chrome")
    await sess.__aenter__()
    return sess, True


async def fetch_json_urls_async(
    sym_urls: List[Tuple[str, str]],
    headers: Dict[str, str],
    parser_fn: Callable[[Any], Tuple[Optional[ArrayBundle], bool]],
    max_concurrency: int,
    max_per_sec: int,
    max_per_min: int,
    limiter_key: str,
    on_auth_error: Callable[[], None],
    on_rate_limit: Callable[[], None],
    should_abort: Callable[[], bool],
    timeout: int = 10,
    max_per_30min: int = 0,
    retry_waves: int = 2,
) -> Tuple[Dict[str, ArrayBundle], Set[str]]:
    """
    Fetch many per-symbol historical JSON endpoints concurrently over a persistent HTTP/2
    AsyncSession bounded by a sliding-window rate limiter, with rate-limit-aware backoff retries.
    """
    if not sym_urls:
        return {}, set()

    try:
        running_loop = asyncio.get_running_loop()
    except RuntimeError:
        running_loop = None
    persistent = yahoo_client._ASYNC_LOOP is not None and running_loop is yahoo_client._ASYNC_LOOP

    limiter = _get_limiter(
        limiter_key,
        running_loop,
        persistent,
        max_per_sec,
        max_per_min,
        max_per_30min=max_per_30min,
    )
    sess, close_ephemeral = await _acquire_session()
    sem = asyncio.Semaphore(max(1, int(max_concurrency)))
    url_by_sym = dict(sym_urls)

    async def _fetch_one(
        sym: str,
        url: str,
        use_sem: asyncio.Semaphore,
    ) -> Tuple[str, Optional[ArrayBundle], bool, Optional[int]]:
        async with use_sem:
            if should_abort():
                return sym, None, True, 401
            await limiter.acquire()
            if should_abort():
                return sym, None, True, 401
            try:
                resp = await sess.get(url, headers=headers, timeout=timeout)
                code = int(resp.status_code)
                if code in (401, 403):
                    on_auth_error()
                    return sym, None, True, code
                if code == 429:
                    await limiter.penalize(_parse_retry_after(resp))
                    on_rate_limit()
                    return sym, None, False, 429
                if code != 200:
                    return sym, None, False, code
                bundle, ok = parser_fn(decode_json_response(resp))
                return sym, bundle, ok, 200
            except Exception:
                return sym, None, False, None

    results: Dict[str, ArrayBundle] = {}
    failed: Set[str] = set()
    rate_limited: Set[str] = set()
    try:
        wave1 = await asyncio.gather(*(_fetch_one(sym, url, sem) for sym, url in sym_urls))
        for sym, bundle, ok, code in wave1:
            if bundle is not None:
                results[sym] = bundle
            elif not ok:
                failed.add(sym)
                if code == 429:
                    rate_limited.add(sym)

        retry_sem = asyncio.Semaphore(max(1, int(max_concurrency) // 4))
        total_retry_waves = max(1, int(retry_waves))
        for wave_idx in range(total_retry_waves):
            if not failed or should_abort():
                break
            if wave_idx > 0 and not rate_limited:
                break
            targets = sorted(failed if wave_idx == 0 else rate_limited)
            if not targets:
                break
            if rate_limited:
                delay = max(limiter.remaining_cooldown(), 1.25 * (wave_idx + 1))
                use_sem = retry_sem
            else:
                delay = 0.5
                use_sem = sem
            await asyncio.sleep(delay)
            wave_res = await asyncio.gather(*(_fetch_one(sym, url_by_sym[sym], use_sem) for sym in targets))
            for sym, bundle, ok, code in wave_res:
                if bundle is not None:
                    results[sym] = bundle
                    failed.discard(sym)
                    rate_limited.discard(sym)
                elif ok:
                    failed.discard(sym)
                    rate_limited.discard(sym)
                elif code != 429:
                    rate_limited.discard(sym)
    finally:
        if close_ephemeral:
            await sess.__aexit__(None, None, None)

    return results, failed
