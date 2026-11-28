"""
Background 5paisa Xstream WebSocket stream manager (MarketFeedV3 over openfeed.5paisa.com).
Maintains thread-safe real-time LTP, cumulative volume, running VWAP (or exchange ATP when present),
and rolling 1-minute volume buckets per symbol for live Vol_Burst_15m calculations.
"""

from __future__ import annotations

import atexit
import base64
from datetime import date, datetime, time
import json
import re
import threading
import time as pytime
from typing import Any, Dict, List, Optional, Sequence

from core.utils import IST, ist_epoch_sec, localize_ist
from providers.common.stream_base import (
    BaseMarketStream,
    secret_fingerprint,
    to_float,
)

try:
    import orjson as _orjson
except ImportError:  # pragma: no cover - optional faster JSON decoder
    _orjson = None

_SUBSCRIBE_BATCH_SIZE = 50
_MAX_RECONNECTS = 10
_ASPNET_DATE_RE = re.compile(r"/Date\((-?\d+)")

_STREAM_LOCK = threading.Lock()
_ACTIVE_STREAMS: Dict[str, "FivePaisaMarketStream"] = {}


def decode_jwt_claims(access_token: str) -> Dict[str, Any]:
    """Decode the unverified JSON payload claims of a 5paisa JWT access_token."""
    token = (access_token or "").strip()
    parts = token.split(".")
    if len(parts) < 2 or not parts[1]:
        return {}
    try:
        b64 = parts[1].replace("-", "+").replace("_", "/")
        padding = "=" * ((4 - len(b64) % 4) % 4)
        raw = base64.b64decode(b64 + padding)
        parsed = _orjson.loads(raw) if _orjson is not None else json.loads(raw.decode("utf-8"))
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def extract_client_code_from_jwt(access_token: str) -> str:
    """Best-effort extraction of 5paisa ClientCode from standard JWT claims."""
    claims = decode_jwt_claims(access_token)
    for key in ("ClientCode", "clientcode", "client_code", "unique_name", "sub"):
        val = str(claims.get(key) or "").strip()
        if val:
            return val
    return ""


def resolve_fivepaisa_ws_url(access_token: str, client_code: str) -> str:
    """Route WebSocket endpoint using RedirectServer claim in JWT ('A'/'B'/'C'/default)."""
    claims = decode_jwt_claims(access_token)
    redirect_srv = str(claims.get("RedirectServer") or "").strip().upper()
    if redirect_srv == "A":
        base = "wss://aopenfeed.5paisa.com/feeds/api/chat"
    elif redirect_srv == "B":
        base = "wss://bopenfeed.5paisa.com/feeds/api/chat"
    else:
        base = "wss://openfeed.5paisa.com/feeds/api/chat"
    return f"{base}?Value1={access_token.strip()}|{client_code.strip()}"


def build_market_feed_subscribe_payloads(
    client_code: str,
    scrip_codes: Sequence[str],
    operation: str = "Subscribe",
    batch_size: int = _SUBSCRIBE_BATCH_SIZE,
) -> List[Dict[str, Any]]:
    """Build chunked MarketFeedV3 JSON payloads (50 scrips per frame by default)."""
    codes = [str(c).strip() for c in scrip_codes if str(c).strip()]
    if not codes:
        return []
    step = max(1, int(batch_size))
    payloads: List[Dict[str, Any]] = []
    for idx in range(0, len(codes), step):
        chunk = codes[idx : idx + step]
        payloads.append(
            {
                "Method": "MarketFeedV3",
                "Operation": operation,
                "ClientCode": client_code.strip(),
                "MarketFeedData": [
                    {
                        "Exch": "N",
                        "ExchType": "C",
                        "ScripCode": int(code) if code.isdigit() else code,
                    }
                    for code in chunk
                ],
            }
        )
    return payloads


def _decode_ws_message(message: Any) -> Any:
    if isinstance(message, (dict, list)):
        return message
    if isinstance(message, (bytes, bytearray, memoryview)):
        try:
            return _orjson.loads(message) if _orjson is not None else json.loads(bytes(message).decode("utf-8"))
        except Exception:
            return None
    if isinstance(message, str):
        text = message.strip()
        if not text:
            return None
        try:
            return _orjson.loads(text) if _orjson is not None else json.loads(text)
        except Exception:
            return None
    return None


def _extract_tick_items(decoded: Any) -> List[Dict[str, Any]]:
    """Extract tick dictionaries from list, Data/MarketFeedData wrapper, or bare dict packets."""
    if isinstance(decoded, list):
        return [item for item in decoded if isinstance(item, dict)]
    if isinstance(decoded, dict):
        for key in ("Data", "data", "MarketFeedData"):
            nested = decoded.get(key)
            if isinstance(nested, list):
                return [item for item in nested if isinstance(item, dict)]
            if isinstance(nested, dict):
                return [nested]
        body = decoded.get("body")
        if isinstance(body, dict) and isinstance(body.get("Data"), list):
            return [item for item in body["Data"] if isinstance(item, dict)]
        if any(k in decoded for k in ("LastRate", "Token", "ScripCode", "Scripcode")):
            return [decoded]
    return []


def _parse_tick_epoch_sec(tick: Dict[str, Any]) -> Optional[int]:
    """Parse TickDt (/Date(ms+offset)/ or ISO) or Time (seconds-since-midnight IST) to UTC epoch seconds."""
    tick_dt = tick.get("TickDt")
    if tick_dt is not None:
        if isinstance(tick_dt, (int, float)) and tick_dt > 1e8:
            return int(tick_dt // 1000) if tick_dt > 1e11 else int(tick_dt)
        raw_dt = str(tick_dt).strip()
        m = _ASPNET_DATE_RE.search(raw_dt)
        if m:
            try:
                ms = int(m.group(1))
                return ms // 1000 if abs(ms) > 10**11 else ms
            except ValueError:
                pass
        if raw_dt:
            try:
                dt = datetime.fromisoformat(raw_dt.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = localize_ist(dt)
                return int(dt.timestamp())
            except ValueError:
                pass

    raw_time = to_float(tick.get("Time"))
    if raw_time is not None and 0 <= raw_time < 86400:
        sec_int = int(raw_time)
        today_ist = datetime.now(IST).date()
        t_wall = time(sec_int // 3600, (sec_int % 3600) // 60, sec_int % 60)
        return ist_epoch_sec(today_ist, t_wall)

    return None


class FivePaisaMarketStream(BaseMarketStream):
    """Thread-safe wrapper around 5paisa MarketFeedV3 WebSocket via websocket-client."""

    def __init__(
        self,
        access_token: str,
        client_code: str,
        key_to_sym: Dict[str, str],
    ) -> None:
        super().__init__(key_to_sym)
        self.access_token = access_token.strip()
        self.client_code = client_code.strip()
        self._sym_base_to_sym: Dict[str, str] = {
            sym.removesuffix(".NS").removesuffix(".BO").strip().upper(): sym
            for sym in self.key_to_sym.values()
        }
        self._ws_app: Any = None
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start background WebSocketApp loop with bounded automatic reconnection."""
        with self._state_lock:
            if self._started and not self._stop_requested:
                return
            self._stop_requested = False
            self._last_error = None
            if not self.key_to_sym:
                self._last_error = "No mapped NSE cash equities to subscribe."
                return
            if not self.client_code:
                self._last_error = "5paisa Client Code is required for MarketFeedV3 WebSocket."
                return
            self._started = True

        t = threading.Thread(target=self._run_loop, name="fivepaisa_ws", daemon=True)
        with self._state_lock:
            self._thread = t
        t.start()

    def ensure_instruments(self, key_to_sym: Dict[str, str]) -> None:
        """Register additional scrip mappings and subscribe new codes if socket is open."""
        new_codes: List[str] = []
        with self._state_lock:
            for code, sym in key_to_sym.items():
                code_str = str(code).strip()
                if code_str not in self.key_to_sym:
                    new_codes.append(code_str)
                self.key_to_sym[code_str] = sym
                self._sym_base_to_sym[sym.removesuffix(".NS").removesuffix(".BO").strip().upper()] = sym
            ws_app = self._ws_app
            connected = self._connected
            client_code = self.client_code

        if new_codes and ws_app is not None and connected:
            self._send_subscribe_batches(ws_app, client_code, sorted(new_codes))

    def _send_subscribe_batches(self, ws_app: Any, client_code: str, codes: Sequence[str]) -> None:
        payloads = build_market_feed_subscribe_payloads(client_code, codes)
        for idx, payload in enumerate(payloads):
            try:
                ws_app.send(json.dumps(payload))
                if idx + 1 < len(payloads):
                    pytime.sleep(0.05)
            except Exception as exc:
                with self._state_lock:
                    self._last_error = f"5paisa WS subscribe error: {exc}"
                break

    def _run_loop(self) -> None:
        import websocket

        ws_url = resolve_fivepaisa_ws_url(self.access_token, self.client_code)
        attempts = 0
        while True:
            with self._state_lock:
                if self._stop_requested:
                    break

            ws_app = websocket.WebSocketApp(
                ws_url,
                on_open=self._on_open,
                on_message=self._on_message_ws,
                on_error=self._on_error,
                on_close=self._on_close,
            )
            with self._state_lock:
                if self._stop_requested:
                    break
                self._ws_app = ws_app

            try:
                ws_app.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as exc:
                with self._state_lock:
                    self._last_error = f"5paisa WS error: {exc}"
            finally:
                with self._state_lock:
                    self._connected = False
                    if self._ws_app is ws_app:
                        self._ws_app = None
                    should_stop = self._stop_requested

            if should_stop:
                break

            attempts += 1
            if attempts > _MAX_RECONNECTS:
                with self._state_lock:
                    self._started = False
                    if not self._last_error:
                        self._last_error = "5paisa WS max reconnect attempts reached."
                break

            wait_s = min(15.0, 2.0 + attempts)
            slept = 0.0
            while slept < wait_s:
                with self._state_lock:
                    if self._stop_requested:
                        return
                pytime.sleep(0.25)
                slept += 0.25

    def stop(self) -> None:
        """Close WebSocket cleanly and signal background reconnect loop to exit."""
        with self._state_lock:
            self._stop_requested = True
            self._connected = False
            self._started = False
            ws_app = self._ws_app
            self._ws_app = None
            thread = self._thread
            self._thread = None

        if ws_app is not None:
            try:
                ws_app.close()
            except Exception:
                pass
        if thread is not None and thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout=1.5)

    def _on_open(self, ws_app: Any) -> None:
        with self._state_lock:
            self._connected = True
            self._last_error = None
            codes = sorted(self.key_to_sym.keys())
            client_code = self.client_code
        if codes:
            self._send_subscribe_batches(ws_app, client_code, codes)

    def _on_close(self, *args: Any) -> None:
        with self._state_lock:
            self._connected = False

    def _on_error(self, *args: Any) -> None:
        error = args[-1] if args else "Unknown WebSocket error"
        err_text = str(error)
        with self._state_lock:
            self._last_error = err_text
            if "401" in err_text or "Unauthorized" in err_text:
                self._connected = False
                self._stop_requested = True

    def _on_message_ws(self, ws_app: Any, message: Any) -> None:
        self._on_message(message)

    def _on_message(self, message: Any) -> None:
        decoded = _decode_ws_message(message)
        items = _extract_tick_items(decoded)
        if not items:
            return

        with self._state_lock:
            self._last_msg_mono = pytime.monotonic()
            for tick in items:
                raw_code = (
                    tick.get("Token")
                    if tick.get("Token") not in (None, "")
                    else tick.get("ScripCode", tick.get("Scripcode"))
                )
                sym = self.key_to_sym.get(str(raw_code).strip()) if raw_code not in (None, "") else None
                if sym is None:
                    tsym = str(tick.get("Symbol") or "").strip().upper()
                    sym = self._sym_base_to_sym.get(tsym)
                if sym is None:
                    continue
                self._ingest_tick_locked(sym, tick)
                self._tick_count += 1

    def _ingest_tick_locked(self, sym: str, tick: Dict[str, Any]) -> None:
        ltp = to_float(tick.get("LastRate") if tick.get("LastRate") is not None else tick.get("LTP"))
        vtt = to_float(tick.get("TotalQty") if tick.get("TotalQty") is not None else tick.get("Volume"))
        day_open = to_float(tick.get("OpenRate") if tick.get("OpenRate") is not None else tick.get("Open"))
        day_high = to_float(tick.get("High"))
        day_low = to_float(tick.get("Low"))
        atp = to_float(
            tick.get("AverageRate")
            if tick.get("AverageRate") is not None
            else tick.get("AvgRate", tick.get("ATP"))
        )

        tick_sec = _parse_tick_epoch_sec(tick)
        feed_date: Optional[date] = (
            datetime.fromtimestamp(tick_sec, IST).date() if tick_sec is not None else None
        )

        self._apply_quote_tick_locked(
            sym=sym,
            feed_date=feed_date,
            tick_sec=tick_sec,
            ltp=ltp,
            day_open=day_open,
            day_high=day_high,
            day_low=day_low,
            atp=atp,
            vtt=vtt,
        )


def get_or_start_fivepaisa_stream(
    access_token: str,
    client_code: str,
    key_to_sym: Dict[str, str],
    auto_start: bool = True,
) -> Optional[FivePaisaMarketStream]:
    """Return singleton FivePaisaMarketStream for (client_code, token), stopping stale streams."""
    token = (access_token or "").strip()
    ccode = (client_code or "").strip() or extract_client_code_from_jwt(token)
    if not token or not ccode:
        return None
    fp = secret_fingerprint(ccode, token)
    stale_streams: List[FivePaisaMarketStream] = []

    with _STREAM_LOCK:
        stream = _ACTIVE_STREAMS.get(fp)
        for other_fp, other_stream in list(_ACTIVE_STREAMS.items()):
            if other_fp != fp:
                stale_streams.append(other_stream)
                _ACTIVE_STREAMS.pop(other_fp, None)
        if stream is None:
            stream = FivePaisaMarketStream(token, ccode, key_to_sym)
            _ACTIVE_STREAMS[fp] = stream

    for stale in stale_streams:
        stale.stop()

    stream.ensure_instruments(key_to_sym)
    if auto_start:
        stream.start()
    return stream


def stop_fivepaisa_stream(access_token: Optional[str] = None, client_code: Optional[str] = None) -> None:
    """Stop the active 5paisa stream matching credentials (or all active 5paisa streams)."""
    to_stop: List[FivePaisaMarketStream] = []
    with _STREAM_LOCK:
        if access_token:
            token = access_token.strip()
            ccode = (client_code or "").strip() or extract_client_code_from_jwt(token)
            if ccode:
                fp = secret_fingerprint(ccode, token)
                s = _ACTIVE_STREAMS.pop(fp, None)
                if s is not None:
                    to_stop.append(s)
            else:
                for k in [k for k, v in _ACTIVE_STREAMS.items() if v.access_token == token]:
                    to_stop.append(_ACTIVE_STREAMS.pop(k))
        else:
            to_stop.extend(_ACTIVE_STREAMS.values())
            _ACTIVE_STREAMS.clear()
    for stream in to_stop:
        stream.stop()


atexit.register(stop_fivepaisa_stream)
