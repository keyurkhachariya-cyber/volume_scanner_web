"""
Upstox API v3 MarketDataProvider:
- Historical & intraday v3 candles over rate-limited HTTP/2 async sessions (with requests fallback).
- Live WebSocket streaming (UpstoxMarketStream) with zero-REST intraday scanning once seeded.
- OAuth dialog URL + auth_code -> access_token exchange helper.
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime, time
import threading
import time as pytime
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import quote as _url_quote, urlencode

import numpy as np

from providers.base import ArrayBundle, TodaySummary
from providers.upstox_instruments import resolve_instrument_keys
from providers.upstox_stream import (
    get_or_start_upstox_stream,
    stop_upstox_stream,
)
from utils import decode_json_response
import yahoo_client

_UPSTOX_API_BASE = "https://api.upstox.com"
_UPSTOX_MAX_CONCURRENCY = 12
_UPSTOX_REQUEST_INTERVAL = 0.032  # ~31 req/s max average, comfortably under 50 req/s & 500 req/min
_SEED_STABLE_WAIT_SECONDS = 1.5


def build_upstox_login_url(client_id: str, redirect_uri: str) -> str:
    """Build the Upstox OAuth2 consent URL for daily token generation."""
    params = urlencode(
        {
            "response_type": "code",
            "client_id": client_id.strip(),
            "redirect_uri": redirect_uri.strip(),
        }
    )
    return f"{_UPSTOX_API_BASE}/v2/login/authorization/dialog?{params}"


def exchange_upstox_auth_code(
    code: str,
    client_id: str,
    client_secret: str,
    redirect_uri: str,
    timeout: int = 10,
) -> Tuple[Optional[str], Optional[str]]:
    """Exchange an Upstox OAuth authorization code for a daily access_token."""
    import requests

    try:
        resp = requests.post(
            f"{_UPSTOX_API_BASE}/v2/login/authorization/token",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded",
                "Api-Version": "2.0",
            },
            data={
                "code": code.strip(),
                "client_id": client_id.strip(),
                "client_secret": client_secret.strip(),
                "redirect_uri": redirect_uri.strip(),
                "grant_type": "authorization_code",
            },
            timeout=timeout,
        )
        payload = decode_json_response(resp)
        if resp.status_code != 200:
            errs = payload.get("errors") if isinstance(payload, dict) else None
            msg = (
                errs[0].get("message")
                if isinstance(errs, list) and errs and isinstance(errs[0], dict)
                else f"HTTP {resp.status_code}"
            )
            return None, f"Upstox login failed: {msg}"
        token = payload.get("access_token") if isinstance(payload, dict) else None
        if not token:
            return None, "Upstox response did not contain an access_token."
        return str(token), None
    except Exception as exc:
        return None, f"Upstox token exchange error: {exc}"


def _parse_upstox_candles(payload: Any) -> Tuple[Optional[ArrayBundle], bool]:
    """
    Parse Upstox v3 candle JSON ([ts_iso, open, high, low, close, volume, oi]) into ascending NumPy arrays.
    Returns (bundle_or_none, succeeded_without_transient_error).
    """
    if not isinstance(payload, dict):
        return None, False
    if payload.get("status") != "success":
        return None, False

    candles = (payload.get("data") or {}).get("candles")
    if not candles or not isinstance(candles, list):
        return None, True

    rows: List[Tuple[int, float, float, float, float, float]] = []
    for c in candles:
        if not isinstance(c, (list, tuple)) or len(c) < 6:
            continue
        raw_ts = c[0]
        if raw_ts is None:
            continue
        try:
            ts_sec = int(datetime.fromisoformat(str(raw_ts)).timestamp())
            o = float("nan") if c[1] is None else float(c[1])
            h = float("nan") if c[2] is None else float(c[2])
            l = float("nan") if c[3] is None else float(c[3])
            cl = float("nan") if c[4] is None else float(c[4])
            v = float("nan") if c[5] is None else float(c[5])
            rows.append((ts_sec, o, h, l, cl, v))
        except (TypeError, ValueError):
            continue

    if not rows:
        return None, True

    # Upstox returns descending newest-first candles; normalize chronologically once
    if len(rows) > 1 and rows[0][0] > rows[-1][0]:
        rows.reverse()
    if len(rows) > 1 and any(rows[i][0] > rows[i + 1][0] for i in range(len(rows) - 1)):
        rows.sort(key=lambda r: r[0])

    return {
        "ts_sec": np.asarray([r[0] for r in rows], dtype=np.int64),
        "open": np.asarray([r[1] for r in rows], dtype=np.float64),
        "high": np.asarray([r[2] for r in rows], dtype=np.float64),
        "low": np.asarray([r[3] for r in rows], dtype=np.float64),
        "close": np.asarray([r[4] for r in rows], dtype=np.float64),
        "volume": np.asarray([r[5] for r in rows], dtype=np.float64),
    }, True


class UpstoxProvider:
    """Upstox v3 Historical/Intraday REST + live MarketDataStreamerV3 provider."""

    provider_id: str = "upstox"
    display_name: str = "Upstox API v3"
    cache_namespace: str = "upstox"
    supports_coalesced_range: bool = False

    def __init__(
        self,
        access_token: str,
        use_live_ws: bool = True,
        eq_map: Optional[Dict[str, str]] = None,
    ) -> None:
        self.access_token = (access_token or "").strip()
        self.use_live_ws = bool(use_live_ws)
        self._eq_map_override = eq_map
        self._lock = threading.Lock()
        self._last_error: Optional[str] = None
        self._auth_failed: bool = False

    def get_last_error(self) -> Optional[str]:
        with self._lock:
            return self._last_error

    def _set_error(self, msg: Optional[str], auth_failed: bool = False) -> None:
        with self._lock:
            self._last_error = msg
            if auth_failed:
                self._auth_failed = True

    def _build_candle_url(
        self,
        instrument_key: str,
        interval: str,
        from_date: Optional[str],
        to_date: Optional[str],
    ) -> str:
        iv_num = "5" if interval == "5m" else "1"
        enc_key = _url_quote(instrument_key, safe="")
        if from_date and to_date:
            # Upstox to_date is inclusive: clamp end if range used exclusive tomorrow date
            to_dt = date.fromisoformat(to_date)
            from_dt = date.fromisoformat(from_date)
            if (to_dt - from_dt).days == 1:
                to_str = from_str = from_dt.isoformat()
            else:
                from_str, to_str = from_dt.isoformat(), to_dt.isoformat()
            return f"{_UPSTOX_API_BASE}/v3/historical-candle/{enc_key}/minutes/{iv_num}/{to_str}/{from_str}"
        return f"{_UPSTOX_API_BASE}/v3/historical-candle/intraday/{enc_key}/minutes/{iv_num}"

    async def _fetch_candles_async(
        self,
        sym_tasks: List[Tuple[str, str, Optional[str], Optional[str]]],
        interval: str,
        timeout: int = 10,
    ) -> Tuple[Dict[str, ArrayBundle], Set[str]]:
        from curl_cffi.requests import AsyncSession

        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.access_token}",
        }
        sem = asyncio.Semaphore(_UPSTOX_MAX_CONCURRENCY)
        pace_lock = asyncio.Lock()
        last_req_mono = [0.0]

        async def _pace() -> None:
            async with pace_lock:
                now_m = pytime.monotonic()
                wait_s = (last_req_mono[0] + _UPSTOX_REQUEST_INTERVAL) - now_m
                if wait_s > 0:
                    await asyncio.sleep(wait_s)
                last_req_mono[0] = pytime.monotonic()

        async def _fetch_one(
            sess: Any, sym: str, ikey: str, s_str: Optional[str], e_str: Optional[str]
        ) -> Tuple[str, Optional[ArrayBundle], bool, Optional[int]]:
            url = self._build_candle_url(ikey, interval, s_str, e_str)
            async with sem:
                with self._lock:
                    if self._auth_failed:
                        return sym, None, True, 401
                await _pace()
                try:
                    resp = await sess.get(url, headers=headers, timeout=timeout)
                    code = int(resp.status_code)
                    if code in (401, 403):
                        self._set_error(
                            "Upstox access token expired or invalid (HTTP 401/403). Generate a fresh daily token.",
                            auth_failed=True,
                        )
                        return sym, None, True, code
                    if code == 429:
                        self._set_error("Upstox API rate limit hit (HTTP 429); retrying briefly.")
                        return sym, None, False, 429
                    if code != 200:
                        return sym, None, False, code
                    bundle, ok = _parse_upstox_candles(decode_json_response(resp))
                    return sym, bundle, ok, 200
                except Exception:
                    return sym, None, False, None

        results: Dict[str, ArrayBundle] = {}
        failed: Set[str] = set()
        task_map = {sym: (ikey, s_str, e_str) for sym, ikey, s_str, e_str in sym_tasks}

        async with AsyncSession(impersonate="chrome") as sess:
            wave1 = await asyncio.gather(
                *(_fetch_one(sess, sym, ikey, s_str, e_str) for sym, ikey, s_str, e_str in sym_tasks)
            )
            for sym, bundle, ok, code in wave1:
                if bundle is not None:
                    results[sym] = bundle
                elif not ok:
                    failed.add(sym)

            if failed and not self._auth_failed:
                await asyncio.sleep(1.0)
                wave2 = await asyncio.gather(
                    *(_fetch_one(sess, sym, *task_map[sym]) for sym in sorted(failed))
                )
                for sym, bundle, ok, _ in wave2:
                    if bundle is not None:
                        results[sym] = bundle
                        failed.discard(sym)
                    elif ok:
                        failed.discard(sym)

        return results, failed

    def fetch_bars(
        self,
        symbol_periods: Iterable[Tuple[str, Optional[str]]],
        interval: str,
        max_workers: int = 8,
        start: Optional[str] = None,
        end: Optional[str] = None,
        chunk_size: Optional[int] = None,
        symbol_ranges: Optional[Dict[str, Tuple[Optional[str], Optional[str]]]] = None,
    ) -> Tuple[Dict[str, ArrayBundle], Set[str]]:
        """Fetch historical or intraday v3 candles from Upstox for resolved NSE_EQ symbols."""
        tasks = list(symbol_periods)
        if not tasks:
            return {}, set()
        if not self.access_token:
            self._set_error("Upstox access token is missing — paste today's token in the sidebar.", auth_failed=True)
            return {}, {sym for sym, _ in tasks}

        try:
            sym_to_key, _ = resolve_instrument_keys((sym for sym, _ in tasks), eq_map=self._eq_map_override)
        except Exception as exc:
            self._set_error(f"Failed loading Upstox NSE instruments: {exc}")
            return {}, {sym for sym, _ in tasks}

        sym_tasks: List[Tuple[str, str, Optional[str], Optional[str]]] = []
        for sym, _ in tasks:
            ikey = sym_to_key.get(sym)
            if not ikey:
                continue
            s_str, e_str = symbol_ranges.get(sym, (start, end)) if symbol_ranges else (start, end)
            sym_tasks.append((sym, ikey, s_str, e_str))

        if not sym_tasks:
            return {}, set()

        try:
            return yahoo_client._run_coro_sync(lambda: self._fetch_candles_async(sym_tasks, interval=interval))
        except Exception as exc:
            self._set_error(f"Upstox candle fetch error: {exc}")
            return {}, {sym for sym, _, _, _ in sym_tasks}

    def fetch_today_summaries(
        self,
        symbols: List[str],
        today: date,
        cutoff: time,
        interval: str,
        max_workers: int = 8,
    ) -> Tuple[Optional[Dict[str, TodaySummary]], Dict[str, Any]]:
        """
        When Live WSS is enabled:
        1. Ensure singleton MarketDataStreamerV3 is connected for all mapped F&O instruments.
        2. Seed 1m rolling volume buckets once per day from /v3/historical-candle/intraday.
        3. Serve real-time in-memory TodaySummary snapshots cleanly; fall back to REST if WS has 0 ticks yet.
        """
        meta: Dict[str, Any] = {"provider": self.provider_id, "mode": "wss" if self.use_live_ws else "rest"}
        if not self.access_token:
            msg = "Upstox access token is missing — paste today's token in the sidebar."
            self._set_error(msg, auth_failed=True)
            meta["error"] = msg
            return {}, meta

        if not self.use_live_ws:
            stop_upstox_stream(self.access_token)
            return None, meta

        try:
            sym_to_key, key_to_sym = resolve_instrument_keys(symbols, eq_map=self._eq_map_override)
        except Exception as exc:
            msg = f"Failed loading Upstox NSE instruments: {exc}"
            self._set_error(msg)
            meta["error"] = msg
            return None, meta

        meta["mapped"] = len(sym_to_key)
        meta["unmapped"] = len(symbols) - len(sym_to_key)

        stream = get_or_start_upstox_stream(self.access_token, key_to_sym, auto_start=True)
        if stream is None:
            return None, meta

        # Seed 15m rolling volume history once on cold start / mid-day launch
        if stream.needs_intraday_seed(today):
            seed_bundles, _ = self.fetch_bars(((sym, "1d") for sym in sym_to_key.keys()), interval="1m")
            if self._auth_failed:
                meta["error"] = self.get_last_error()
                return {}, meta
            if seed_bundles:
                stream.seed_from_intraday_bundles(today, seed_bundles)

        # Wait briefly on initial connect so first live snapshot arrives before falling back
        if stream.get_status().get("ticks", 0) == 0:
            deadline = pytime.monotonic() + _SEED_STABLE_WAIT_SECONDS
            while pytime.monotonic() < deadline and stream.get_status().get("ticks", 0) == 0:
                if stream.get_status().get("error"):
                    break
                pytime.sleep(0.1)

        bar_interval_sec = 300 if interval == "5m" else 60
        summaries = stream.get_today_summaries(symbols, today, cutoff, bar_interval_sec=bar_interval_sec)
        status = stream.get_status()
        meta.update(status)
        if status.get("error"):
            self._set_error(status["error"])

        if summaries:
            return summaries, meta

        # If WS has not emitted today's data yet (e.g. post-close historical review or initial handshake),
        # fall back gracefully to REST intraday candles so Scan Now still populates results.
        meta["mode"] = "rest_fallback"
        return None, meta
