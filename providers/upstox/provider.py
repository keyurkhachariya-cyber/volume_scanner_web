"""
Upstox API v3 MarketDataProvider:
- Historical & intraday v3 candles over a shared persistent HTTP/2 AsyncSession + sliding rate limiter.
- Fast 1-call Batch Full Market Quotes (/v2/market-quote/quotes, up to 500 keys per GET) for instant
  REST snapshots and immediate pre-tick WSS hydration.
- Live WebSocket streaming (UpstoxMarketStream) with non-blocking background 1m Vol_Burst_15m seeding.
- OAuth dialog URL + auth_code -> access_token exchange helper.
"""

from __future__ import annotations

from datetime import date, datetime, time
import threading
import time as pytime
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import quote as _url_quote, urlencode

import numpy as np

from core.holidays import is_market_open, is_wss_active_window
from core.utils import IST, decode_json_response, localize_ist
from providers.base import ArrayBundle, TodaySummary
from providers.common.http_runtime import fetch_json_urls_async
from providers.common.stream_base import (
    parse_quote_ts_sec,
    secret_fingerprint,
    snapshots_to_summaries,
    to_float,
)
from providers.upstox.instruments import resolve_instrument_keys
from providers.upstox.stream import (
    get_or_start_upstox_stream,
    stop_upstox_stream,
)
import providers.yahoo.client as yahoo_client

_UPSTOX_API_BASE = "https://api.upstox.com"
_UPSTOX_MAX_CONCURRENCY = 16
_UPSTOX_MAX_PER_SEC = 35     # Upstox v3 limit: 50 req/sec (leave burst margin)
_UPSTOX_MAX_PER_MIN = 380    # Upstox v3 limit: 500 req/min
_UPSTOX_MAX_PER_30MIN = 1800 # Upstox v3 limit: 2,000 req/30 min
_UPSTOX_BATCH_QUOTE_MAX = 500
_MARKET_OPEN_WALL = time(9, 15)
_MARKET_CLOSE_WALL = time(15, 30)
_PRE_SEED_SKIP_CUTOFF = time(9, 20)
_EMPTY_TODAY_COOLDOWN_SEC = 30.0

_SEED_LOCK = threading.Lock()
_INFLIGHT_SEEDS: Set[Tuple[str, date]] = set()
_EMPTY_TODAY_UNTIL: Dict[Tuple[str, date], float] = {}


def build_upstox_login_url(client_id: str, redirect_uri: str, state: str = "") -> str:
    """Build the Upstox OAuth2 consent URL for daily token generation."""
    params_dict = {
        "response_type": "code",
        "client_id": client_id.strip(),
        "redirect_uri": redirect_uri.strip(),
    }
    if state.strip():
        params_dict["state"] = state.strip()
    return f"{_UPSTOX_API_BASE}/v2/login/authorization/dialog?{urlencode(params_dict)}"


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
            dt = datetime.fromisoformat(str(raw_ts).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = localize_ist(dt)
            ts_sec = int(dt.timestamp())
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


def _parse_upstox_batch_quotes(
    payload: Any,
    key_to_sym: Dict[str, str],
    today: date,
) -> List[Dict[str, Any]]:
    """
    Parse GET /v2/market-quote/quotes payload into normalized quote snapshots for `today`,
    preferring last_trade_time over query timestamp so stale prior-day trades never leak into today.
    """
    if not isinstance(payload, dict) or payload.get("status") != "success":
        return []
    data = payload.get("data")
    if not isinstance(data, dict) or not data:
        return []

    sym_by_base = {
        sym.removesuffix(".NS").removesuffix(".BO").strip().upper(): sym
        for sym in key_to_sym.values()
    }
    snapshots: List[Dict[str, Any]] = []

    for entry_key, entry in data.items():
        if not isinstance(entry, dict):
            continue
        ikey = str(entry.get("instrument_token") or "").strip()
        sym = key_to_sym.get(ikey) if ikey else None
        if sym is None:
            tsym = str(entry.get("symbol") or "").strip().upper()
            if not tsym and ":" in str(entry_key):
                tsym = str(entry_key).split(":", 1)[1].strip().upper()
            sym = sym_by_base.get(tsym)
        if sym is None:
            continue

        # Prefer actual exchange last_trade_time over wrapper server timestamp
        tick_sec = parse_quote_ts_sec(entry.get("last_trade_time"))
        if tick_sec is None:
            tick_sec = parse_quote_ts_sec(entry.get("timestamp"))
        if tick_sec is None:
            continue

        tick_dt = datetime.fromtimestamp(tick_sec, IST)
        if tick_dt.date() != today or tick_dt.time() < _MARKET_OPEN_WALL:
            continue

        vtt = to_float(entry.get("volume"))
        if vtt is None or vtt <= 0.0:
            continue

        ltp = to_float(entry.get("last_price"))
        atp = to_float(entry.get("average_price"))
        ohlc = entry.get("ohlc") if isinstance(entry.get("ohlc"), dict) else {}
        day_open = to_float(ohlc.get("open")) or ltp
        day_high = to_float(ohlc.get("high")) or ltp
        day_low = to_float(ohlc.get("low")) or ltp

        snapshots.append(
            {
                "sym": sym,
                "feed_date": today,
                "tick_sec": tick_sec,
                "ltp": ltp,
                "day_open": day_open,
                "day_high": day_high,
                "day_low": day_low,
                "atp": atp,
                "vtt": vtt,
            }
        )

    return snapshots


class UpstoxProvider:
    """Upstox v3 Historical/Intraday REST + 1-call Batch Quote + live MarketDataStreamerV3 provider."""

    provider_id: str = "upstox"
    display_name: str = "Upstox API v3"
    cache_namespace: str = "upstox"
    supports_coalesced_range: bool = False
    supports_replay_coalesced_range: bool = True

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

    def _is_auth_failed(self) -> bool:
        with self._lock:
            return self._auth_failed

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
            to_dt = date.fromisoformat(to_date)
            from_dt = date.fromisoformat(from_date)
            if (to_dt - from_dt).days == 1:
                to_str = from_str = from_dt.isoformat()
            else:
                from_str, to_str = from_dt.isoformat(), to_dt.isoformat()
            return f"{_UPSTOX_API_BASE}/v3/historical-candle/{enc_key}/minutes/{iv_num}/{to_str}/{from_str}"
        return f"{_UPSTOX_API_BASE}/v3/historical-candle/intraday/{enc_key}/minutes/{iv_num}"

    def _fetch_batch_quotes_sync(
        self,
        sym_to_key: Dict[str, str],
        key_to_sym: Dict[str, str],
        today: date,
        timeout: int = 10,
    ) -> Tuple[List[Dict[str, Any]], bool]:
        """Fetch /v2/market-quote/quotes in 500-key batches (1 single HTTP GET for <=500 F&O stocks)."""
        keys = list(sym_to_key.values())
        if not keys:
            return [], True

        from providers.common.http_runtime import _acquire_session

        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.access_token}",
        }
        chunks = [
            ",".join(keys[i : i + _UPSTOX_BATCH_QUOTE_MAX])
            for i in range(0, len(keys), _UPSTOX_BATCH_QUOTE_MAX)
        ]

        if not self._is_auth_failed():
            self._set_error(None)

        async def _run() -> Tuple[List[Dict[str, Any]], bool]:
            import asyncio

            sess, close_ephemeral = await _acquire_session()
            snapshots: List[Dict[str, Any]] = []
            ok_all = True
            try:
                for chunk_keys in chunks:
                    url = f"{_UPSTOX_API_BASE}/v2/market-quote/quotes?instrument_key={_url_quote(chunk_keys, safe=',|')}"
                    chunk_ok = False
                    for attempt in range(2):
                        resp = await sess.get(url, headers=headers, timeout=timeout)
                        code = int(resp.status_code)
                        if code in (401, 403):
                            self._set_error(
                                "Upstox access token expired or invalid (HTTP 401/403). Generate a fresh daily token.",
                                auth_failed=True,
                            )
                            return [], False
                        if code == 429:
                            self._set_error("Upstox API rate limit hit (HTTP 429); retrying briefly.")
                            if attempt == 0:
                                await asyncio.sleep(1.25)
                                continue
                            break
                        if code != 200:
                            if attempt == 0 and code >= 500:
                                await asyncio.sleep(0.5)
                                continue
                            break
                        snapshots.extend(_parse_upstox_batch_quotes(decode_json_response(resp), key_to_sym, today))
                        chunk_ok = True
                        break
                    if not chunk_ok:
                        ok_all = False
                if ok_all and not self._is_auth_failed():
                    self._set_error(None)
                return snapshots, ok_all
            finally:
                if close_ephemeral:
                    await sess.__aexit__(None, None, None)

        try:
            return yahoo_client._run_coro_sync(_run)
        except Exception:
            return [], False

    def _start_background_intraday_seed(
        self,
        stream: Any,
        symbols: List[str],
        today: date,
    ) -> None:
        """Backfill 1-minute rolling volume buckets once in background without blocking initial scan render."""
        seed_key = (secret_fingerprint(self.access_token), today)
        with _SEED_LOCK:
            if seed_key in _INFLIGHT_SEEDS or not stream.needs_intraday_seed(today):
                return
            _INFLIGHT_SEEDS.add(seed_key)

        def _worker() -> None:
            try:
                seed_bundles, failed = self.fetch_bars(((sym, "1d") for sym in symbols), interval="1m")
                if seed_bundles:
                    stream.seed_from_intraday_bundles(today, seed_bundles)
                elif not failed and not self._is_auth_failed():
                    stream.mark_seeded_date(today)
            except Exception:
                pass
            finally:
                with _SEED_LOCK:
                    _INFLIGHT_SEEDS.discard(seed_key)

        threading.Thread(target=_worker, name="upstox_1m_seed", daemon=True).start()

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
        """Fetch historical or intraday v3 candles concurrently over persistent HTTP/2 AsyncSession."""
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

        sym_urls: List[Tuple[str, str]] = []
        for sym, _ in tasks:
            ikey = sym_to_key.get(sym)
            if not ikey:
                continue
            s_str, e_str = symbol_ranges.get(sym, (start, end)) if symbol_ranges else (start, end)
            sym_urls.append((sym, self._build_candle_url(ikey, interval, s_str, e_str)))

        if not sym_urls:
            return {}, set()

        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.access_token}",
        }
        limiter_key = f"upstox:{secret_fingerprint(self.access_token)}"
        if not self._is_auth_failed():
            self._set_error(None)

        try:
            results, failed = yahoo_client._run_coro_sync(
                lambda: fetch_json_urls_async(
                    sym_urls=sym_urls,
                    headers=headers,
                    parser_fn=_parse_upstox_candles,
                    max_concurrency=_UPSTOX_MAX_CONCURRENCY,
                    max_per_sec=_UPSTOX_MAX_PER_SEC,
                    max_per_min=_UPSTOX_MAX_PER_MIN,
                    max_per_30min=_UPSTOX_MAX_PER_30MIN,
                    limiter_key=limiter_key,
                    on_auth_error=lambda: self._set_error(
                        "Upstox access token expired or invalid (HTTP 401/403). Generate a fresh daily token.",
                        auth_failed=True,
                    ),
                    on_rate_limit=lambda: self._set_error(
                        "Upstox API rate limit hit (HTTP 429); retrying briefly."
                    ),
                    should_abort=self._is_auth_failed,
                )
            )
            if self._is_auth_failed():
                failed |= {sym for sym, _ in sym_urls if sym not in results}
            elif not failed:
                self._set_error(None)
            return results, failed
        except Exception as exc:
            self._set_error(f"Upstox candle fetch error: {exc}")
            return {}, {sym for sym, _ in sym_urls}

    def fetch_today_summaries(
        self,
        symbols: List[str],
        today: date,
        cutoff: time,
        interval: str,
        max_workers: int = 8,
    ) -> Tuple[Optional[Dict[str, TodaySummary]], Dict[str, Any]]:
        """
        Serve live TodaySummary records cleanly without redundant 218-call REST loops:
        - Live WSS mode: serves in-memory ticks immediately; uses 1-call Batch Quote (~0.2s) if
          WS has 0 ticks yet, and seeds 1m Vol_Burst_15m history non-blockingly in the background.
        - REST mode: uses 1-call Batch Quote (/v2/market-quote/quotes) instead of 218 intraday calls.
        - Off-hours / weekends: avoids duplicate empty intraday bursts via short empty-day cooldown.
        """
        meta: Dict[str, Any] = {"provider": self.provider_id, "mode": "wss" if self.use_live_ws else "batch_quote"}
        if not self.access_token:
            msg = "Upstox access token is missing — paste today's token in the sidebar."
            self._set_error(msg, auth_failed=True)
            meta["error"] = msg
            return {}, meta

        try:
            sym_to_key, key_to_sym = resolve_instrument_keys(symbols, eq_map=self._eq_map_override)
        except Exception as exc:
            msg = f"Failed loading Upstox NSE instruments: {exc}"
            self._set_error(msg)
            meta["error"] = msg
            return None, meta

        meta["mapped"] = len(sym_to_key)
        meta["unmapped"] = len(symbols) - len(sym_to_key)

        tok_fp = secret_fingerprint(self.access_token)
        now_ist = datetime.combine(today, cutoff, tzinfo=IST)
        market_open_now = is_market_open(now_ist, background_refresh=True)
        if _MARKET_OPEN_WALL <= cutoff <= _MARKET_CLOSE_WALL:
            wss_active_now = market_open_now
        else:
            wss_active_now = is_wss_active_window(now_ist, background_refresh=True)

        # Pure REST mode: single 500-key Batch Quote request (~0.2s vs 218 candle GETs)
        if not self.use_live_ws:
            stop_upstox_stream(self.access_token)
            cooldown_key = (tok_fp, today)
            with _SEED_LOCK:
                if pytime.monotonic() < _EMPTY_TODAY_UNTIL.get(cooldown_key, 0.0):
                    meta["mode"] = "closed_cached"
                    return {}, meta
            snapshots, ok = self._fetch_batch_quotes_sync(sym_to_key, key_to_sym, today)
            if self._is_auth_failed():
                meta["error"] = self.get_last_error()
                return {}, meta
            if snapshots:
                return snapshots_to_summaries(snapshots, cutoff), meta
            if ok and not market_open_now:
                with _SEED_LOCK:
                    _EMPTY_TODAY_UNTIL[cooldown_key] = pytime.monotonic() + _EMPTY_TODAY_COOLDOWN_SEC
                meta["mode"] = "closed_cached"
                return {}, meta
            meta["mode"] = "rest_fallback"
            return None, meta

        # Only start/keep WebSocket connected inside 09:05-15:35 IST on trading days; auto-stop off-hours
        stream = get_or_start_upstox_stream(self.access_token, key_to_sym, auto_start=wss_active_now)
        if stream is None:
            return None, meta

        # Background 1m seed (skipped right near open < 09:20 or when market has 0 today data yet)
        if market_open_now and stream.needs_intraday_seed(today):
            if cutoff < _PRE_SEED_SKIP_CUTOFF:
                stream.mark_seeded_date(today)
            else:
                self._start_background_intraday_seed(stream, list(sym_to_key.keys()), today)

        bar_interval_sec = 300 if interval == "5m" else 60
        summaries = stream.get_today_summaries(symbols, today, cutoff, bar_interval_sec=bar_interval_sec)

        # If WS has 0 today summaries yet (cold connect, weekend, or post-close), use 1-call Batch Quote (~0.2s)
        if not summaries:
            cooldown_key = (tok_fp, today)
            in_cooldown = False
            if not market_open_now:
                with _SEED_LOCK:
                    in_cooldown = pytime.monotonic() < _EMPTY_TODAY_UNTIL.get(cooldown_key, 0.0)
            if in_cooldown:
                meta.update(stream.get_status())
                meta["mode"] = "closed_cached"
                return {}, meta

            snapshots, ok = self._fetch_batch_quotes_sync(sym_to_key, key_to_sym, today)
            if self._is_auth_failed():
                meta["error"] = self.get_last_error()
                return {}, meta
            if snapshots:
                stream.ingest_quote_snapshots(snapshots)
                summaries = stream.get_today_summaries(symbols, today, cutoff, bar_interval_sec=bar_interval_sec)
                if not market_open_now and stream.needs_intraday_seed(today):
                    # Post-close review on a trading day: backfill 1m burst quietly in background too
                    self._start_background_intraday_seed(stream, list(sym_to_key.keys()), today)
                meta["mode"] = "batch_quote"
            elif ok and not market_open_now:
                with _SEED_LOCK:
                    _EMPTY_TODAY_UNTIL[cooldown_key] = pytime.monotonic() + _EMPTY_TODAY_COOLDOWN_SEC
                meta.update(stream.get_status())
                meta["mode"] = "closed_cached"
                return {}, meta

        status = stream.get_status()
        meta.update(status)
        if status.get("error"):
            self._set_error(status["error"])

        if summaries:
            if not wss_active_now and meta.get("mode") == "wss":
                meta["mode"] = "post_close_cached"
            return summaries, meta

        meta["mode"] = "rest_fallback"
        return None, meta
