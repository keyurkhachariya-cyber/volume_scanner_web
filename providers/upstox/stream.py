"""
Background Upstox v3 WebSocket stream manager (MarketDataStreamerV3 in 'full' mode).
Maintains thread-safe real-time quotes, cumulative volume, exchange ATP (VWAP),
and rolling 1-minute volume buckets per symbol for live Vol_Burst_15m calculations.
"""

from __future__ import annotations

import atexit
from datetime import date, datetime
import threading
import time as pytime
from typing import Any, Dict, List, Optional, Tuple

from core.utils import IST
from providers.common.stream_base import (
    BaseMarketStream,
    register_stream_stopper,
    secret_fingerprint as _token_fingerprint,
    to_float as _to_float,
    unregister_stream_stopper,
)

_STREAM_LOCK = threading.Lock()
_ACTIVE_STREAMS: Dict[str, "UpstoxMarketStream"] = {}


def _extract_market_ff(feed_entry: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Extract (marketFF_or_equiv, ltpc) cleanly from a MessageToDict Feed entry."""
    full_feed = feed_entry.get("fullFeed")
    if isinstance(full_feed, dict):
        market_ff = full_feed.get("marketFF") or full_feed.get("indexFF") or {}
        if isinstance(market_ff, dict):
            ltpc = market_ff.get("ltpc") if isinstance(market_ff.get("ltpc"), dict) else {}
            return market_ff, ltpc
    flwg = feed_entry.get("firstLevelWithGreeks")
    if isinstance(flwg, dict):
        ltpc = flwg.get("ltpc") if isinstance(flwg.get("ltpc"), dict) else {}
        return flwg, ltpc
    ltpc = feed_entry.get("ltpc") if isinstance(feed_entry.get("ltpc"), dict) else {}
    return {}, ltpc


class UpstoxMarketStream(BaseMarketStream):
    """Thread-safe wrapper around Upstox MarketDataStreamerV3 ('full' mode)."""

    def __init__(self, access_token: str, key_to_sym: Dict[str, str]) -> None:
        super().__init__(key_to_sym)
        self.access_token = access_token.strip()
        self._streamer: Any = None

    def start(self) -> None:
        """Start MarketDataStreamerV3 in background thread if not already active."""
        with self._state_lock:
            if self._started and not self._stop_requested:
                return
            self._stop_requested = False
            self._last_error = None
            keys = sorted(self.key_to_sym.keys())
            if not keys:
                self._last_error = "No mapped NSE_EQ instruments to subscribe."
                return

        import upstox_client

        configuration = upstox_client.Configuration()
        configuration.access_token = self.access_token
        api_client = upstox_client.ApiClient(configuration)
        streamer = upstox_client.MarketDataStreamerV3(api_client, keys, "full")
        streamer.auto_reconnect(True, interval=3, retry_count=10)
        streamer.on("open", self._on_open)
        streamer.on("message", self._on_message)
        streamer.on("error", self._on_error)
        streamer.on("close", self._on_close)
        streamer.on("autoReconnectStopped", self._on_reconnect_stopped)

        with self._state_lock:
            self._streamer = streamer
            self._started = True
        streamer.connect()

    def ensure_instruments(self, key_to_sym: Dict[str, str]) -> None:
        """Register additional symbol mappings and subscribe new keys if live."""
        new_keys: List[str] = []
        with self._state_lock:
            for ikey, sym in key_to_sym.items():
                if ikey not in self.key_to_sym:
                    new_keys.append(ikey)
                self.key_to_sym[ikey] = sym
            streamer = self._streamer
            connected = self._connected

        if new_keys and streamer is not None and connected:
            try:
                streamer.subscribe(new_keys, "full")
            except Exception as exc:
                with self._state_lock:
                    self._last_error = f"Subscribe error: {exc}"

    def stop(self) -> None:
        """Cleanly disconnect underlying WebSocket and disable reconnect."""
        with self._state_lock:
            self._stop_requested = True
            self._connected = False
            self._started = False
            streamer = self._streamer
            self._streamer = None

        if streamer is not None:
            try:
                streamer.auto_reconnect(False)
            except Exception:
                pass
            try:
                streamer.disconnect()
            except Exception:
                pass

    def _on_open(self, *args: Any) -> None:
        with self._state_lock:
            self._connected = True
            self._last_error = None

    def _on_close(self, *args: Any) -> None:
        with self._state_lock:
            self._connected = False

    def _on_error(self, error: Any) -> None:
        err_text = str(error)
        with self._state_lock:
            self._last_error = err_text
            if "401" in err_text or "Unauthorized" in err_text:
                self._connected = False
                self._stop_requested = True

    def _on_reconnect_stopped(self, reason: Any) -> None:
        with self._state_lock:
            self._connected = False
            if not self._last_error:
                self._last_error = f"Upstox WS reconnect stopped: {reason}"

    def _on_message(self, message: Dict[str, Any]) -> None:
        if not isinstance(message, dict):
            return
        feeds = message.get("feeds")
        if not isinstance(feeds, dict) or not feeds:
            return
        msg_ts_ms = _to_float(message.get("currentTs"))

        with self._state_lock:
            self._last_msg_mono = pytime.monotonic()
            for ikey, feed_entry in feeds.items():
                sym = self.key_to_sym.get(ikey)
                if sym is None or not isinstance(feed_entry, dict):
                    continue
                self._ingest_feed_locked(sym, feed_entry, msg_ts_ms)
                self._tick_count += 1

    def _ingest_feed_locked(
        self, sym: str, feed_entry: Dict[str, Any], msg_ts_ms: Optional[float]
    ) -> None:
        market_ff, ltpc = _extract_market_ff(feed_entry)
        ltp = _to_float(ltpc.get("ltp"))
        ltt_ms = _to_float(ltpc.get("ltt"))

        # Check proto fields on marketFF directly (plus defensive eFeedDetails fallback)
        efd = market_ff.get("eFeedDetails") if isinstance(market_ff.get("eFeedDetails"), dict) else {}
        atp = _to_float(market_ff.get("atp"))
        if atp is None:
            atp = _to_float(efd.get("atp"))
        vtt = _to_float(market_ff.get("vtt"))
        if vtt is None:
            vtt = _to_float(efd.get("vtt"))

        day_open = day_high = day_low = day_vol_fallback = None
        day_ts_sec: Optional[int] = None
        i1_bars: List[Tuple[int, float]] = []

        ohlc_wrap = market_ff.get("marketOHLC")
        if isinstance(ohlc_wrap, dict):
            for candle in ohlc_wrap.get("ohlc") or []:
                if not isinstance(candle, dict):
                    continue
                iv = str(candle.get("interval") or "").strip()
                c_ts_ms = _to_float(candle.get("ts"))
                if iv == "1d":
                    day_open = _to_float(candle.get("open"))
                    day_high = _to_float(candle.get("high"))
                    day_low = _to_float(candle.get("low"))
                    day_vol_fallback = _to_float(candle.get("vol"))
                    if c_ts_ms is not None and c_ts_ms > 0:
                        day_ts_sec = int(c_ts_ms // 1000)
                elif iv == "I1":
                    c_vol = _to_float(candle.get("vol"))
                    if c_ts_ms is not None and c_ts_ms > 0 and c_vol is not None:
                        i1_bars.append((int(c_ts_ms // 1000), c_vol))

        if vtt is None:
            vtt = day_vol_fallback

        raw_ts_ms = ltt_ms if (ltt_ms is not None and ltt_ms > 0) else msg_ts_ms
        tick_sec = int(raw_ts_ms // 1000) if (raw_ts_ms is not None and raw_ts_ms > 0) else None
        feed_date: Optional[date] = None
        if day_ts_sec is not None:
            feed_date = datetime.fromtimestamp(day_ts_sec, IST).date()
        elif tick_sec is not None:
            feed_date = datetime.fromtimestamp(tick_sec, IST).date()

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
            explicit_minute_bars=i1_bars or None,
        )


def get_or_start_upstox_stream(
    access_token: str,
    key_to_sym: Dict[str, str],
    auto_start: bool = True,
) -> Optional[UpstoxMarketStream]:
    """Return a singleton UpstoxMarketStream for this token, cleanly stopping stale token streams."""
    token = (access_token or "").strip()
    if not token:
        return None
    fp = _token_fingerprint(token)
    stale_streams: List[UpstoxMarketStream] = []

    with _STREAM_LOCK:
        stream = _ACTIVE_STREAMS.get(fp)
        for other_fp, other_stream in list(_ACTIVE_STREAMS.items()):
            if other_fp != fp:
                stale_streams.append(other_stream)
                _ACTIVE_STREAMS.pop(other_fp, None)
        if stream is None:
            stream = UpstoxMarketStream(token, key_to_sym)
            _ACTIVE_STREAMS[fp] = stream

    for stale in stale_streams:
        stale.stop()

    stream.ensure_instruments(key_to_sym)
    if auto_start:
        stream.start()
        register_stream_stopper("upstox", stop_upstox_stream)
    else:
        stream.stop()
        with _STREAM_LOCK:
            has_started = any(getattr(s, "_started", False) for s in _ACTIVE_STREAMS.values())
        if not has_started:
            unregister_stream_stopper("upstox")
    return stream


def stop_upstox_stream(access_token: Optional[str] = None) -> None:
    """Stop the stream matching access_token (or all active Upstox streams)."""
    to_stop: List[UpstoxMarketStream] = []
    with _STREAM_LOCK:
        if access_token:
            fp = _token_fingerprint(access_token.strip())
            s = _ACTIVE_STREAMS.pop(fp, None)
            if s is not None:
                to_stop.append(s)
        else:
            to_stop.extend(_ACTIVE_STREAMS.values())
            _ACTIVE_STREAMS.clear()
        empty = not _ACTIVE_STREAMS
    for stream in to_stop:
        stream.stop()
    if empty:
        unregister_stream_stopper("upstox")


atexit.register(stop_upstox_stream)
