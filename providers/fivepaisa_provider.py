"""
5paisa Xstream Open API MarketDataProvider:
- Historical & intraday V2 OHLCV candles over a shared persistent HTTP/2 AsyncSession + sliding rate limiter.
- Live WebSocket streaming (FivePaisaMarketStream / MarketFeedV3) with non-blocking / deduped 1m seeding.
- Headless TOTP+PIN session login (TOTPLogin -> GetAccessToken) & OAuth login URL helpers.
"""

from __future__ import annotations

import base64
from datetime import date, datetime, time
import hashlib
import hmac
import re
import struct
import threading
import time as pytime
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urlencode

import numpy as np

from holidays import is_market_open
from providers.base import ArrayBundle, TodaySummary
from providers.fivepaisa_instruments import resolve_scrip_codes
from providers.fivepaisa_stream import (
    extract_client_code_from_jwt,
    get_or_start_fivepaisa_stream,
    stop_fivepaisa_stream,
)
from providers.http_runtime import fetch_json_urls_async
from providers.stream_base import secret_fingerprint
from utils import IST, decode_json_response, localize_ist
import yahoo_client

_FIVEPAISA_VENDOR_BASE = "https://Openapi.5paisa.com/VendorsAPI/Service1.svc"
_FIVEPAISA_OAUTH_URL = "https://dev-openapi.5paisa.com/WebVendorLogin/VLogin/Index"
_FIVEPAISA_HISTORICAL_BASE = "https://openapi.5paisa.com/V2/historical"
_FIVEPAISA_SUBSCRIPTION_KEY = "c89fab8d895a426d9e00db380b433027"
_FIVEPAISA_MAX_CONCURRENCY = 16
_FIVEPAISA_MAX_PER_SEC = 22    # 5paisa limit: 25 req/sec
_FIVEPAISA_MAX_PER_MIN = 1200  # 5paisa limit: 1,500 req/min
_SEED_STABLE_WAIT_SECONDS = 1.0
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


class FivePaisaProvider:
    """5paisa Xstream V2 Historical REST + live MarketFeedV3 WebSocket provider."""

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
    ) -> None:
        self.access_token = (access_token or "").strip()
        self.client_code = (client_code or "").strip() or extract_client_code_from_jwt(self.access_token)
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
            if (to_dt - from_dt).days == 1:
                from_str = to_str = from_dt.isoformat()
            else:
                from_str, to_str = from_dt.isoformat(), to_dt.isoformat()
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
            return yahoo_client._run_coro_sync(
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
        Serve live TodaySummary records cleanly without duplicate 218-call fallback loops:
        - Live WSS: starts MarketFeedV3, seeds 1m Vol_Burst_15m in background when WS ticks exist,
          or fetches once and serves directly from seeded state if WS has 0 ticks yet.
        - Off-hours / weekends: avoids sleep delays and caches empty today checks cleanly.
        """
        meta: Dict[str, Any] = {"provider": self.provider_id, "mode": "wss" if self.use_live_ws else "rest"}
        if not self.access_token:
            msg = "5paisa access token is missing — login via TOTP/OAuth or paste token."
            self._set_error(msg, auth_failed=True)
            meta["error"] = msg
            return {}, meta

        now_ist = datetime.combine(today, cutoff, tzinfo=IST)
        market_open_now = is_market_open(now_ist, background_refresh=True)
        cooldown_key = (secret_fingerprint(self.client_code, self.access_token), today)

        if not self.use_live_ws:
            stop_fivepaisa_stream(self.access_token, self.client_code)
            if not market_open_now:
                with _SEED_LOCK:
                    if pytime.monotonic() < _EMPTY_TODAY_UNTIL.get(cooldown_key, 0.0):
                        meta["mode"] = "closed_cached"
                        return {}, meta
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

        stream = get_or_start_fivepaisa_stream(
            self.access_token, self.client_code, code_to_sym, auto_start=True
        )
        if stream is None:
            return None, meta

        bar_interval_sec = 300 if interval == "5m" else 60
        seeded_bundles: Dict[str, ArrayBundle] = {}

        if market_open_now:
            if stream.get_status().get("ticks", 0) == 0:
                deadline = pytime.monotonic() + _SEED_STABLE_WAIT_SECONDS
                while pytime.monotonic() < deadline and stream.get_status().get("ticks", 0) == 0:
                    if stream.get_status().get("error"):
                        break
                    pytime.sleep(0.1)

            if stream.needs_intraday_seed(today):
                if cutoff < _PRE_SEED_SKIP_CUTOFF:
                    stream.mark_seeded_date(today)
                elif stream.get_status().get("ticks", 0) > 0:
                    self._start_background_intraday_seed(stream, list(sym_to_code.keys()), today)
                else:
                    seeded_bundles, failed = self.fetch_bars(((sym, "1d") for sym in sym_to_code.keys()), interval="1m")
                    if self._is_auth_failed():
                        meta["error"] = self.get_last_error()
                        return {}, meta
                    if seeded_bundles:
                        stream.seed_from_intraday_bundles(today, seeded_bundles)
                        meta["mode"] = "rest_seed"
                    elif not failed:
                        stream.mark_seeded_date(today)
        else:
            # Closed market: avoid 1s WS wait and duplicate empty fallback burst
            with _SEED_LOCK:
                in_cooldown = pytime.monotonic() < _EMPTY_TODAY_UNTIL.get(cooldown_key, 0.0)
            if in_cooldown:
                meta.update(stream.get_status())
                meta["mode"] = "closed_cached"
                return {}, meta

            if stream.needs_intraday_seed(today):
                seeded_bundles, failed = self.fetch_bars(((sym, "1d") for sym in sym_to_code.keys()), interval="1m")
                if self._is_auth_failed():
                    meta["error"] = self.get_last_error()
                    return {}, meta
                if seeded_bundles:
                    stream.seed_from_intraday_bundles(today, seeded_bundles)
                    meta["mode"] = "rest_seed"
                elif not failed:
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
            return summaries, meta

        # If we already fetched today's bars cleanly and got nothing valid for today, skip duplicate fetch_bars
        if seeded_bundles or not market_open_now:
            return {}, meta

        meta["mode"] = "rest_fallback"
        return None, meta
