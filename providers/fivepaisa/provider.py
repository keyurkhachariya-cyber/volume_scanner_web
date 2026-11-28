"""
5paisa Xstream Open API MarketDataProvider:
- Historical & intraday V2 OHLCV candles over a shared persistent HTTP/2 AsyncSession + sliding rate limiter.
- Live WebSocket streaming (FivePaisaMarketStream / MarketFeedV3) with non-blocking / deduped 1m seeding.
- Headless TOTP+PIN session login (TOTPLogin -> GetAccessToken) & OAuth login URL helpers.
"""

from __future__ import annotations

import asyncio
import base64
from datetime import date, datetime, time, timedelta
import hashlib
import hmac
import re
import struct
import threading
import time as pytime
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urlencode

import numpy as np

from core.holidays import get_holidays, is_market_open, is_wss_active_window
from core.utils import IST, decode_json_response, localize_ist
from providers.base import ArrayBundle, TodaySummary
from providers.common.http_runtime import fetch_json_urls_async
from providers.common.stream_base import (
    parse_quote_ts_sec,
    secret_fingerprint,
    snapshots_to_summaries,
    to_float,
)
from providers.fivepaisa.instruments import resolve_scrip_codes
from providers.fivepaisa.stream import (
    decode_jwt_claims,
    extract_client_code_from_jwt,
    get_or_start_fivepaisa_stream,
    stop_fivepaisa_stream,
)
import providers.yahoo.client as yahoo_client

_FIVEPAISA_VENDOR_BASE = "https://Openapi.5paisa.com/VendorsAPI/Service1.svc"
_FIVEPAISA_BATCH_FEED_URL = f"{_FIVEPAISA_VENDOR_BASE}/V1/MarketFeed"
_FIVEPAISA_OAUTH_URL = "https://dev-openapi.5paisa.com/WebVendorLogin/VLogin/Index"
_FIVEPAISA_HISTORICAL_BASE = "https://openapi.5paisa.com/V2/historical"
_FIVEPAISA_SUBSCRIPTION_KEY = "c89fab8d895a426d9e00db380b433027"
_FIVEPAISA_MAX_CONCURRENCY = 16
_FIVEPAISA_MAX_PER_SEC = 22    # 5paisa historical limit: 25 req/sec
_FIVEPAISA_MAX_PER_MIN = 1200  # 5paisa historical limit: 1,500 req/min
_FIVEPAISA_BATCH_FEED_MAX = 50         # 5paisa V1/MarketFeed safe chunk size (vs Upstox 500)
_FIVEPAISA_BATCH_FEED_CONCURRENCY = 5  # ~218 F&O stocks fit into 5 concurrent POST calls
_FIVEPAISA_BATCH_FEED_PER_SEC = 10
_FIVEPAISA_BATCH_FEED_PER_MIN = 45     # 5paisa non-order data feed limit (~50 req/min)
_MARKET_OPEN_WALL = time(9, 15)
_MARKET_CLOSE_WALL = time(15, 30)
_PRE_SEED_SKIP_CUTOFF = time(9, 20)
_EMPTY_TODAY_COOLDOWN_SEC = 30.0

_SEED_LOCK = threading.Lock()
_INFLIGHT_SEEDS: Set[Tuple[str, date]] = set()
_EMPTY_TODAY_UNTIL: Dict[Tuple[str, date], float] = {}


def generate_totp_code(totp_or_secret: str, for_time: Optional[float] = None) -> str:
    """
    Accept either a ready 6-digit TOTP code or a Base32 TOTP secret key and return the 6-digit code
    using pure stdlib RFC 6238 HMAC-SHA1 (zero external pyotp dependency).
    """
    cleaned = re.sub(r"[\s-]+", "", (totp_or_secret or "").strip())
    if not cleaned:
        raise ValueError("TOTP code or secret is empty.")
    if len(cleaned) == 6 and cleaned.isdigit():
        return cleaned

    b32 = cleaned.upper()
    b32 += "=" * ((8 - len(b32) % 8) % 8)
    try:
        key = base64.b32decode(b32, casefold=True)
    except Exception as exc:
        raise ValueError(f"Invalid TOTP secret format: {exc}") from exc

    now_sec = pytime.time() if for_time is None else float(for_time)
    counter = int(now_sec // 30)
    msg = struct.pack(">Q", counter)
    digest = hmac.new(key, msg, hashlib.sha1).digest()
    offset = digest[-1] & 0x0F
    binary = struct.unpack(">I", digest[offset : offset + 4])[0] & 0x7FFFFFFF
    return f"{binary % 1_000_000:06d}"


def build_fivepaisa_login_url(vendor_key: str, response_url: str, state: str = "") -> str:
    """Build the 5paisa OAuth WebVendorLogin URL for browser-based daily authentication."""
    params: Dict[str, str] = {
        "VendorKey": vendor_key.strip(),
        "ResponseURL": response_url.strip(),
    }
    if state.strip():
        params["state"] = state.strip()
    return f"{_FIVEPAISA_OAUTH_URL}?{urlencode(params)}"


def exchange_fivepaisa_request_token(
    request_token: str,
    user_key: str,
    encryption_key: str,
    user_id: str,
    timeout: int = 12,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Exchange a 5paisa RequestToken for a daily JWT AccessToken via GetAccessToken.
    Returns (access_token, client_code, error_message).
    """
    import requests

    try:
        resp = requests.post(
            f"{_FIVEPAISA_VENDOR_BASE}/GetAccessToken",
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            json={
                "head": {"Key": user_key.strip()},
                "body": {
                    "RequestToken": request_token.strip(),
                    "EncryKey": encryption_key.strip(),
                    "UserId": user_id.strip(),
                },
            },
            timeout=timeout,
        )
        payload = decode_json_response(resp)
        body = payload.get("body") if isinstance(payload, dict) else None
        if resp.status_code != 200 or not isinstance(body, dict):
            return None, None, f"5paisa GetAccessToken failed (HTTP {resp.status_code})"

        token = str(body.get("AccessToken") or "").strip()
        msg = str(body.get("Message") or "").strip()
        ccode = str(body.get("ClientCode") or "").strip() or extract_client_code_from_jwt(token)
        if not token:
            return None, None, f"5paisa GetAccessToken failed: {msg or 'No AccessToken returned.'}"
        return token, ccode, None
    except Exception as exc:
        return None, None, f"5paisa GetAccessToken error: {exc}"


def login_fivepaisa_totp(
    client_code: str,
    totp_or_secret: str,
    pin: str,
    user_key: str,
    encryption_key: str,
    user_id: str,
    timeout: int = 12,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Perform headless 5paisa TOTPLogin -> GetAccessToken flow using ClientCode + TOTP/Secret + PIN.
    Returns (access_token, client_code, error_message).
    """
    import requests

    try:
        totp_code = generate_totp_code(totp_or_secret)
    except ValueError as exc:
        return None, None, str(exc)

    try:
        resp = requests.post(
            f"{_FIVEPAISA_VENDOR_BASE}/TOTPLogin",
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            json={
                "head": {"Key": user_key.strip()},
                "body": {
                    "Email_ID": client_code.strip(),
                    "TOTP": totp_code,
                    "PIN": pin.strip(),
                },
            },
            timeout=timeout,
        )
        payload = decode_json_response(resp)
        body = payload.get("body") if isinstance(payload, dict) else None
        if resp.status_code != 200 or not isinstance(body, dict):
            return None, None, f"5paisa TOTPLogin failed (HTTP {resp.status_code})"

        req_token = str(body.get("RequestToken") or "").strip()
        status = body.get("Status")
        if status not in (0, "0", None) or not req_token:
            msg = str(body.get("Message") or "Invalid ClientCode, TOTP, or PIN.")
            return None, None, f"5paisa TOTPLogin failed: {msg}"

        token, resolved_code, err = exchange_fivepaisa_request_token(
            request_token=req_token,
            user_key=user_key,
            encryption_key=encryption_key,
            user_id=user_id,
            timeout=timeout,
        )
        return token, (resolved_code or client_code.strip()), err
    except Exception as exc:
        return None, None, f"5paisa TOTP login error: {exc}"


def _parse_fivepaisa_candles(payload: Any) -> Tuple[Optional[ArrayBundle], bool]:
    """
    Parse 5paisa V2 historical candle JSON ([ts_iso, open, high, low, close, volume]) into ascending arrays.
    Returns (bundle_or_none, succeeded_without_transient_error).
    """
    if not isinstance(payload, dict):
        return None, False

    status = str(payload.get("status") or "").strip().lower()
    if status and status != "success":
        return None, False

    data = payload.get("data")
    candles = data.get("candles") if isinstance(data, dict) else payload.get("candles")
    if candles is None and status == "success":
        return None, True
    if not isinstance(candles, list):
        return None, False
    if not candles:
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


def _extract_user_key_from_jwt(access_token: str) -> str:
    """Best-effort extraction of 5paisa UserKey/AppKey from JWT claims if omitted."""
    claims = decode_jwt_claims(access_token)
    for key in ("UserKey", "userkey", "user_key", "AppKey", "appkey", "app_key"):
        val = str(claims.get(key) or "").strip()
        if val:
            return val
    return ""


def _parse_fivepaisa_market_feed(
    payload: Any,
    code_to_sym: Dict[str, str],
    today: date,
) -> Tuple[List[Dict[str, Any]], bool]:
    """
    Parse POST /V1/MarketFeed JSON into normalized quote snapshots for `today`.
    Returns (snapshots, succeeded) where succeeded=False suppresses empty-day caching on API errors.
    """
    if not isinstance(payload, dict):
        return [], False
    head = payload.get("head")
    if isinstance(head, dict) and head.get("status") not in (0, "0", None):
        return [], False
    body = payload.get("body")
    if not isinstance(body, dict):
        return [], False
    if body.get("Status") not in (0, "0", None):
        return [], False

    raw_items = body.get("Data")
    if not isinstance(raw_items, list):
        raw_items = body.get("MarketFeedData") if raw_items is None else raw_items
    if raw_items is None:
        return [], True
    if not isinstance(raw_items, list) or not raw_items:
        return [], True

    sym_by_base = {
        sym.removesuffix(".NS").removesuffix(".BO").strip().upper(): sym
        for sym in code_to_sym.values()
    }
    snapshots: List[Dict[str, Any]] = []

    for entry in raw_items:
        if not isinstance(entry, dict):
            continue
        raw_code = (
            entry.get("Token")
            if entry.get("Token") not in (None, "")
            else entry.get("ScripCode", entry.get("Scripcode"))
        )
        sym = code_to_sym.get(str(raw_code).strip()) if raw_code not in (None, "") else None
        if sym is None:
            tsym = str(entry.get("Symbol") or "").strip().upper()
            sym = sym_by_base.get(tsym)
        if sym is None:
            continue

        # Require dated exchange tick timestamp (LastTradedTime / TickDt) to avoid prior-day leakage
        tick_sec = None
        for ts_key in ("LastTradedTime", "LastTradeTime", "TickDt"):
            tick_sec = parse_quote_ts_sec(entry.get(ts_key))
            if tick_sec is not None:
                break
        if tick_sec is None:
            continue

        tick_dt = datetime.fromtimestamp(tick_sec, IST)
        if tick_dt.date() != today or tick_dt.time() < _MARKET_OPEN_WALL:
            continue

        vtt = to_float(entry.get("TotalQty") if entry.get("TotalQty") is not None else entry.get("Volume"))
        if vtt is None or vtt <= 0.0:
            continue

        ltp = to_float(entry.get("LastRate") if entry.get("LastRate") is not None else entry.get("LTP"))
        atp = to_float(
            entry.get("AverageRate")
            if entry.get("AverageRate") is not None
            else (entry.get("AvgRate") if entry.get("AvgRate") is not None else entry.get("ATP"))
        )
        day_open = to_float(entry.get("OpenRate") if entry.get("OpenRate") is not None else entry.get("Open")) or ltp
        day_high = to_float(entry.get("High")) or ltp
        day_low = to_float(entry.get("Low")) or ltp

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

    return snapshots, True


def _resolve_market_windows(today: date, cutoff: time) -> Tuple[bool, bool]:
    """Return (market_open_now, wss_active_now) for a given IST date and time."""
    now_ist = datetime.combine(today, cutoff, tzinfo=IST)
    market_open_now = is_market_open(now_ist, background_refresh=True)
    if _MARKET_OPEN_WALL <= cutoff <= _MARKET_CLOSE_WALL:
        wss_active_now = market_open_now
    else:
        wss_active_now = is_wss_active_window(now_ist, background_refresh=True)
    return market_open_now, wss_active_now


def _allow_batch_quote_today(today: date, cutoff: time, market_open_now: bool) -> bool:
    """
    Skip V1/MarketFeed on pre-open (<09:15) or closed weekends/holidays because
    5paisa stamps prior-session closing quotes with today's TickDt when closed.
    """
    if market_open_now:
        return cutoff >= _MARKET_OPEN_WALL
    if cutoff < _MARKET_OPEN_WALL:
        return False
    return today.weekday() < 5 and today not in get_holidays(background_refresh=True)


class FivePaisaProvider:
    """5paisa Xstream V2 Historical REST + 50-scrip V1/MarketFeed Batch Quote + MarketFeedV3 WSS provider."""

    provider_id: str = "fivepaisa"
    display_name: str = "5paisa Xstream"
    cache_namespace: str = "fivepaisa"
    supports_coalesced_range: bool = False

    def __init__(
        self,
        access_token: str,
        client_code: str = "",
        use_live_ws: bool = True,
        eq_map: Optional[Dict[str, str]] = None,
        user_key: str = "",
    ) -> None:
        self.access_token = (access_token or "").strip()
        self.client_code = (client_code or "").strip() or extract_client_code_from_jwt(self.access_token)
        self.user_key = (user_key or "").strip() or _extract_user_key_from_jwt(self.access_token)
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
        scrip_code: str,
        interval: str,
        from_date: Optional[str],
        to_date: Optional[str],
    ) -> str:
        iv = "5m" if interval == "5m" else "1m"
        if from_date and to_date:
            from_dt = date.fromisoformat(from_date)
            to_dt = date.fromisoformat(to_date)
            # Callers pass exclusive end dates (day + 1); 5paisa V2 historical end= is inclusive.
            inc_end = to_dt - timedelta(days=1) if to_dt > from_dt else from_dt
            from_str, to_str = from_dt.isoformat(), inc_end.isoformat()
        elif from_date:
            from_str = to_str = date.fromisoformat(from_date).isoformat()
        else:
            from_str = to_str = datetime.now(IST).date().isoformat()
        return f"{_FIVEPAISA_HISTORICAL_BASE}/N/C/{scrip_code}/{iv}?from={from_str}&end={to_str}"

    def _start_background_intraday_seed(self, stream: Any, symbols: List[str], today: date) -> None:
        """Backfill 1-minute rolling volume buckets once in background without blocking initial scan."""
        seed_key = (secret_fingerprint(self.client_code, self.access_token), today)
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

        threading.Thread(target=_worker, name="fivepaisa_1m_seed", daemon=True).start()

    def _seed_stream_sync(
        self,
        stream: Any,
        symbols: Iterable[str],
        today: date,
        meta: Dict[str, Any],
    ) -> Tuple[Dict[str, ArrayBundle], Set[str], bool]:
        """Fetch today's 1m candles once and seed stream state; returns (bundles, failed, auth_failed)."""
        seeded_bundles, failed = self.fetch_bars(((sym, "1d") for sym in symbols), interval="1m")
        if self._is_auth_failed():
            meta["error"] = self.get_last_error()
            return seeded_bundles, failed, True
        if seeded_bundles:
            stream.seed_from_intraday_bundles(today, seeded_bundles)
            meta["mode"] = "rest_seed"
        return seeded_bundles, failed, False

    def _fetch_batch_quotes_sync(
        self,
        sym_to_code: Dict[str, str],
        code_to_sym: Dict[str, str],
        today: date,
        timeout: int = 10,
    ) -> Tuple[List[Dict[str, Any]], bool]:
        """
        Fetch POST /V1/MarketFeed in 50-scrip chunks concurrently (5 POSTs for ~218 F&O stocks)
        on the shared persistent HTTP/2 session, matching Upstox Batch Quote speed (~0.25s)
        within 5paisa's 50-scrip and rate-limit constraints.
        """
        codes = [str(c).strip() for c in sym_to_code.values() if str(c).strip()]
        if not codes:
            return [], True
        if not (self.access_token and self.client_code and self.user_key):
            return [], False

        from providers.common.http_runtime import _acquire_session, _get_limiter

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"bearer {self.access_token}",
        }
        chunks = [
            codes[i : i + _FIVEPAISA_BATCH_FEED_MAX]
            for i in range(0, len(codes), _FIVEPAISA_BATCH_FEED_MAX)
        ]
        limiter_key = f"fivepaisa_mf:{secret_fingerprint(self.user_key, self.client_code, self.access_token)}"

        async def _run() -> Tuple[List[Dict[str, Any]], bool]:
            try:
                running_loop = asyncio.get_running_loop()
            except RuntimeError:
                running_loop = None
            persistent = yahoo_client._ASYNC_LOOP is not None and running_loop is yahoo_client._ASYNC_LOOP
            limiter = _get_limiter(
                limiter_key,
                running_loop,
                persistent,
                _FIVEPAISA_BATCH_FEED_PER_SEC,
                _FIVEPAISA_BATCH_FEED_PER_MIN,
            )
            sess, close_ephemeral = await _acquire_session()
            sem = asyncio.Semaphore(_FIVEPAISA_BATCH_FEED_CONCURRENCY)

            async def _post_chunk(chunk_idx: int) -> Tuple[int, List[Dict[str, Any]], bool, bool]:
                async with sem:
                    if self._is_auth_failed():
                        return chunk_idx, [], False, False
                    await limiter.acquire()
                    if self._is_auth_failed():
                        return chunk_idx, [], False, False
                    payload = {
                        "head": {"Key": self.user_key},
                        "body": {
                            "ClientCode": self.client_code,
                            "MarketFeedData": [
                                {"Exch": "N", "ExchType": "C", "ScripCode": code, "ScripData": ""}
                                for code in chunks[chunk_idx]
                            ],
                            "LastRequestTime": "/Date(0)/",
                            "RefreshRate": "H",
                        },
                    }
                    try:
                        resp = await sess.post(_FIVEPAISA_BATCH_FEED_URL, headers=headers, json=payload, timeout=timeout)
                        code = int(resp.status_code)
                        if code in (401, 403):
                            self._set_error(
                                "5paisa access token expired or invalid (HTTP 401/403). Run TOTP/OAuth login again.",
                                auth_failed=True,
                            )
                            return chunk_idx, [], False, False
                        if code == 429:
                            self._set_error("5paisa API rate limit hit (HTTP 429); retrying briefly.")
                            return chunk_idx, [], False, True
                        if code != 200:
                            return chunk_idx, [], False, code >= 500
                        snaps, ok = _parse_fivepaisa_market_feed(decode_json_response(resp), code_to_sym, today)
                        return chunk_idx, snaps, ok, False
                    except Exception:
                        return chunk_idx, [], False, True

            snapshots: List[Dict[str, Any]] = []
            retry_indices: List[int] = []
            ok_all = True
            try:
                wave1 = await asyncio.gather(*(_post_chunk(i) for i in range(len(chunks))))
                for idx, chunk_snaps, ok, retryable in wave1:
                    if ok:
                        snapshots.extend(chunk_snaps)
                    elif self._is_auth_failed():
                        return [], False
                    else:
                        ok_all = False
                        if retryable:
                            retry_indices.append(idx)

                if retry_indices and not self._is_auth_failed():
                    await asyncio.sleep(0.5)
                    wave2 = await asyncio.gather(*(_post_chunk(i) for i in retry_indices))
                    retried_ok = True
                    for _, chunk_snaps, ok, _ in wave2:
                        if ok:
                            snapshots.extend(chunk_snaps)
                        else:
                            retried_ok = False
                    # Only clear wave1 failure if every failed chunk was retried and succeeded cleanly
                    ok_all = retried_ok and (len(retry_indices) == sum(1 for _, _, ok, _ in wave1 if not ok))

                return snapshots, (ok_all and not self._is_auth_failed())
            finally:
                if close_ephemeral:
                    await sess.__aexit__(None, None, None)

        try:
            return yahoo_client._run_coro_sync(_run)
        except Exception:
            return [], False

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
        """Fetch historical or today's V2 candles concurrently over persistent HTTP/2 AsyncSession."""
        tasks = list(symbol_periods)
        if not tasks:
            return {}, set()
        if not self.access_token:
            self._set_error("5paisa access token is missing — login via TOTP/OAuth or paste token.", auth_failed=True)
            return {}, {sym for sym, _ in tasks}

        try:
            sym_to_code, _ = resolve_scrip_codes((sym for sym, _ in tasks), eq_map=self._eq_map_override)
        except Exception as exc:
            self._set_error(f"Failed loading 5paisa ScripMaster: {exc}")
            return {}, {sym for sym, _ in tasks}

        sym_urls: List[Tuple[str, str]] = []
        for sym, _ in tasks:
            code = sym_to_code.get(sym)
            if not code:
                continue
            s_str, e_str = symbol_ranges.get(sym, (start, end)) if symbol_ranges else (start, end)
            sym_urls.append((sym, self._build_candle_url(code, interval, s_str, e_str)))

        if not sym_urls:
            return {}, set()

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.access_token}",
            "Ocp-Apim-Subscription-Key": _FIVEPAISA_SUBSCRIPTION_KEY,
        }
        limiter_key = f"fivepaisa:{secret_fingerprint(self.client_code, self.access_token)}"

        try:
            results, failed = yahoo_client._run_coro_sync(
                lambda: fetch_json_urls_async(
                    sym_urls=sym_urls,
                    headers=headers,
                    parser_fn=_parse_fivepaisa_candles,
                    max_concurrency=_FIVEPAISA_MAX_CONCURRENCY,
                    max_per_sec=_FIVEPAISA_MAX_PER_SEC,
                    max_per_min=_FIVEPAISA_MAX_PER_MIN,
                    limiter_key=limiter_key,
                    on_auth_error=lambda: self._set_error(
                        "5paisa access token expired or invalid (HTTP 401/403). Run TOTP/OAuth login again.",
                        auth_failed=True,
                    ),
                    on_rate_limit=lambda: self._set_error(
                        "5paisa API rate limit hit (HTTP 429); retrying briefly."
                    ),
                    should_abort=self._is_auth_failed,
                )
            )
            if self._is_auth_failed():
                failed |= {sym for sym, _ in sym_urls if sym not in results}
            return results, failed
        except Exception as exc:
            self._set_error(f"5paisa candle fetch error: {exc}")
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
        Serve live TodaySummary records matching Upstox's fast path within 5paisa API limits:
        - Live WSS mode: serves in-memory ticks immediately; uses 50-scrip chunked V1/MarketFeed
          Batch Quote (~0.25s, 5 POSTs) when WS has 0 summaries yet and backfills 1m Vol_Burst_15m
          non-blockingly in the background. Falls back cleanly to 1m REST seed if UserKey is absent.
        - REST mode: uses 50-scrip Batch MarketFeed (~0.25s) instead of 218 per-scrip GETs when UserKey is set.
        - Off-hours / weekends: avoids duplicate empty bursts via short empty-day cooldown.
        """
        can_batch_feed = bool(self.user_key and self.client_code)
        meta: Dict[str, Any] = {
            "provider": self.provider_id,
            "mode": "wss" if self.use_live_ws else ("batch_quote" if can_batch_feed else "rest"),
        }
        if not self.access_token:
            msg = "5paisa access token is missing — login via TOTP/OAuth or paste token."
            self._set_error(msg, auth_failed=True)
            meta["error"] = msg
            return {}, meta

        market_open_now, wss_active_now = _resolve_market_windows(today, cutoff)
        cooldown_key = (secret_fingerprint(self.client_code, self.access_token), today)

        # Pure REST mode: fast 5-call V1/MarketFeed batch quote (~0.25s vs 218 candle GETs) when UserKey is available
        if not self.use_live_ws:
            stop_fivepaisa_stream(self.access_token, self.client_code)
            if not market_open_now:
                with _SEED_LOCK:
                    if pytime.monotonic() < _EMPTY_TODAY_UNTIL.get(cooldown_key, 0.0):
                        meta["mode"] = "closed_cached"
                        return {}, meta
            if can_batch_feed:
                if not _allow_batch_quote_today(today, cutoff, market_open_now):
                    with _SEED_LOCK:
                        _EMPTY_TODAY_UNTIL[cooldown_key] = pytime.monotonic() + _EMPTY_TODAY_COOLDOWN_SEC
                    meta["mode"] = "closed_cached"
                    return {}, meta
                try:
                    sym_to_code, code_to_sym = resolve_scrip_codes(symbols, eq_map=self._eq_map_override)
                except Exception as exc:
                    msg = f"Failed loading 5paisa ScripMaster: {exc}"
                    self._set_error(msg)
                    meta["error"] = msg
                    meta["mode"] = "rest_fallback"
                    return None, meta
                meta["mapped"] = len(sym_to_code)
                meta["unmapped"] = len(symbols) - len(sym_to_code)
                snapshots, ok = self._fetch_batch_quotes_sync(sym_to_code, code_to_sym, today)
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

        if not self.client_code:
            msg = "5paisa Client Code is missing for WebSocket streaming (switching to REST historical fallback)."
            self._set_error(msg)
            meta["mode"] = "rest_fallback"
            meta["error"] = msg
            return None, meta

        try:
            sym_to_code, code_to_sym = resolve_scrip_codes(symbols, eq_map=self._eq_map_override)
        except Exception as exc:
            msg = f"Failed loading 5paisa ScripMaster: {exc}"
            self._set_error(msg)
            meta["error"] = msg
            return None, meta

        meta["mapped"] = len(sym_to_code)
        meta["unmapped"] = len(symbols) - len(sym_to_code)

        # Only connect MarketFeedV3 WebSocket inside 09:05-15:35 IST on trading days; auto-stop off-hours
        stream = get_or_start_fivepaisa_stream(
            self.access_token, self.client_code, code_to_sym, auto_start=wss_active_now
        )
        if stream is None:
            return None, meta

        bar_interval_sec = 300 if interval == "5m" else 60
        summaries = stream.get_today_summaries(symbols, today, cutoff, bar_interval_sec=bar_interval_sec)
        fetched_today_once = False

        if summaries:
            if market_open_now and stream.needs_intraday_seed(today):
                if cutoff < _PRE_SEED_SKIP_CUTOFF:
                    stream.mark_seeded_date(today)
                else:
                    self._start_background_intraday_seed(stream, list(sym_to_code.keys()), today)
        else:
            in_cooldown = False
            if not market_open_now:
                with _SEED_LOCK:
                    in_cooldown = pytime.monotonic() < _EMPTY_TODAY_UNTIL.get(cooldown_key, 0.0)
            if in_cooldown:
                meta.update(stream.get_status())
                meta["mode"] = "closed_cached"
                return {}, meta

            batch_ok = False
            if can_batch_feed:
                if not _allow_batch_quote_today(today, cutoff, market_open_now):
                    with _SEED_LOCK:
                        _EMPTY_TODAY_UNTIL[cooldown_key] = pytime.monotonic() + _EMPTY_TODAY_COOLDOWN_SEC
                    meta.update(stream.get_status())
                    meta["mode"] = "closed_cached"
                    return {}, meta
                snapshots, batch_ok = self._fetch_batch_quotes_sync(sym_to_code, code_to_sym, today)
                if self._is_auth_failed():
                    meta["error"] = self.get_last_error()
                    return {}, meta
                if snapshots:
                    stream.ingest_quote_snapshots(snapshots)
                    summaries = stream.get_today_summaries(symbols, today, cutoff, bar_interval_sec=bar_interval_sec)
                    if stream.needs_intraday_seed(today):
                        if market_open_now and cutoff < _PRE_SEED_SKIP_CUTOFF:
                            stream.mark_seeded_date(today)
                        else:
                            # Non-blocking background 1m VWAP & Vol_Burst_15m backfill (mirrors Upstox)
                            self._start_background_intraday_seed(stream, list(sym_to_code.keys()), today)
                    meta["mode"] = "batch_quote"
                elif batch_ok and not market_open_now:
                    with _SEED_LOCK:
                        _EMPTY_TODAY_UNTIL[cooldown_key] = pytime.monotonic() + _EMPTY_TODAY_COOLDOWN_SEC
                    meta.update(stream.get_status())
                    meta["mode"] = "closed_cached"
                    return {}, meta

            # Fallback to single-pass 1m historical REST seed if Batch Feed is unavailable or rejected
            if not summaries and not batch_ok and stream.needs_intraday_seed(today):
                if market_open_now and cutoff < _PRE_SEED_SKIP_CUTOFF:
                    stream.mark_seeded_date(today)
                else:
                    fetched_today_once = True
                    seeded_bundles, failed, auth_err = self._seed_stream_sync(
                        stream, sym_to_code.keys(), today, meta
                    )
                    if auth_err:
                        return {}, meta
                    if not seeded_bundles and not failed:
                        if market_open_now:
                            stream.mark_seeded_date(today)
                        else:
                            with _SEED_LOCK:
                                _EMPTY_TODAY_UNTIL[cooldown_key] = pytime.monotonic() + _EMPTY_TODAY_COOLDOWN_SEC
                            meta.update(stream.get_status())
                            meta["mode"] = "closed_cached"
                            return {}, meta
                    summaries = stream.get_today_summaries(symbols, today, cutoff, bar_interval_sec=bar_interval_sec)

        status = stream.get_status()
        meta.update(status)
        if status.get("error"):
            self._set_error(status["error"])

        if summaries:
            if not wss_active_now and meta.get("mode") == "wss":
                meta["mode"] = "post_close_cached"
            return summaries, meta

        # If we already fetched today's bars (or market is closed), skip duplicate fetch_bars in scanner
        if fetched_today_once or not market_open_now:
            return {}, meta

        meta["mode"] = "rest_fallback"
        return None, meta
