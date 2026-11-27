"""
Background Upstox v3 WebSocket stream manager (MarketDataStreamerV3 in 'full' mode).
Maintains thread-safe real-time quotes, cumulative volume, exchange ATP (VWAP),
and rolling 1-minute volume buckets per symbol for live Vol_Burst_15m calculations.
"""

from __future__ import annotations

import atexit
from datetime import date, datetime, time
import hashlib
import math
import threading
import time as pytime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np

from providers.base import ArrayBundle, TodaySummary
from utils import IST, ist_epoch_sec, time_to_sec

_MARKET_OPEN_SEC = 9 * 3600 + 15 * 60
_STREAM_LOCK = threading.Lock()
_ACTIVE_STREAMS: Dict[str, "UpstoxMarketStream"] = {}


def _to_float(val: Any) -> Optional[float]:
    if val is None or val == "":
        return None
    try:
        num = float(val)
        return None if math.isnan(num) else num
    except (TypeError, ValueError):
        return None


def _token_fingerprint(access_token: str) -> str:
    return hashlib.sha256(access_token.encode("utf-8")).hexdigest()[:16]


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


def _compute_burst_from_minute_buckets(
    minute_vols: Dict[int, float],
    vol_today: float,
    cutoff_bound_sec: int,
    bar_interval_sec: int,
) -> Optional[float]:
    """Compute trailing active-window volume burst from 1-minute buckets."""
    if not minute_vols or vol_today <= 0.0:
        return None
    valid_secs = sorted(ts for ts, v in minute_vols.items() if ts <= cutoff_bound_sec and v is not None)
    if not valid_secs:
        return None

    step_sec = max(60, int(bar_interval_sec))
    if step_sec >= 300:
        agg: Dict[int, float] = {}
        for ts in valid_secs:
            bucket = (ts // step_sec) * step_sec
            agg[bucket] = agg.get(bucket, 0.0) + max(0.0, float(minute_vols[ts]))
        series = [agg[k] for k in sorted(agg.keys())]
    else:
        series = [max(0.0, float(minute_vols[k])) for k in valid_secs]

    recent_bars = max(1, 900 // step_sec)
    if len(series) <= recent_bars:
        return None
    recent_avg = sum(series[-recent_bars:]) / float(recent_bars)
    session_avg = vol_today / float(len(series))
    return (recent_avg / session_avg) if session_avg > 0.0 else None


class UpstoxMarketStream:
    """Thread-safe wrapper around Upstox MarketDataStreamerV3 ('full' mode)."""

    def __init__(self, access_token: str, key_to_sym: Dict[str, str]) -> None:
        self.access_token = access_token.strip()
        self.key_to_sym: Dict[str, str] = dict(key_to_sym)
        self._state_lock = threading.Lock()
        self._streamer: Any = None
        self._started: bool = False
        self._connected: bool = False
        self._stop_requested: bool = False
        self._last_error: Optional[str] = None
        self._last_msg_mono: float = 0.0
        self._tick_count: int = 0
        self._seeded_date: Optional[date] = None
        self._quotes_by_sym: Dict[str, Dict[str, Any]] = {}

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

        state = self._quotes_by_sym.get(sym)
        if state is None or (feed_date is not None and state.get("feed_date") not in (None, feed_date)):
            state = {
                "feed_date": feed_date,
                "ltp": None,
                "day_open": None,
                "day_high": None,
                "day_low": None,
                "atp": None,
                "vtt": None,
                "last_vtt": None,
                "last_ts_sec": None,
                "minute_vols": {},
            }
            self._quotes_by_sym[sym] = state

        if feed_date is not None:
            state["feed_date"] = feed_date
        if ltp is not None:
            state["ltp"] = ltp
        if day_open is not None and day_open > 0:
            state["day_open"] = day_open
        if day_high is not None and day_high > 0:
            state["day_high"] = day_high
        if day_low is not None and day_low > 0:
            state["day_low"] = day_low
        if atp is not None and atp > 0:
            state["atp"] = atp
        if tick_sec is not None:
            prev_ts = state["last_ts_sec"]
            state["last_ts_sec"] = tick_sec if prev_ts is None else max(prev_ts, tick_sec)

        minute_vols = state["minute_vols"]
        if i1_bars:
            for b_sec, b_vol in i1_bars:
                minute_vols[b_sec] = max(float(minute_vols.get(b_sec, 0.0)), float(b_vol))

        if vtt is not None and vtt >= 0:
            prev_vtt = state["last_vtt"]
            if not i1_bars and prev_vtt is not None and vtt > prev_vtt and tick_sec is not None:
                m_sec = (tick_sec // 60) * 60
                minute_vols[m_sec] = minute_vols.get(m_sec, 0.0) + (vtt - prev_vtt)
            state["vtt"] = vtt if state["vtt"] is None else max(state["vtt"], vtt)
            state["last_vtt"] = state["vtt"]

    def needs_intraday_seed(self, today: date) -> bool:
        with self._state_lock:
            return self._seeded_date != today

    def seed_from_intraday_bundles(self, today: date, bundles: Dict[str, ArrayBundle]) -> None:
        """Seed today's 1-minute volume buckets, open/high/low, and VWAP from REST intraday bars."""
        day_start_sec = ist_epoch_sec(today, time(0, 0))
        open_sec = day_start_sec + _MARKET_OPEN_SEC
        day_end_sec = day_start_sec + 86400

        with self._state_lock:
            for sym, bundle in bundles.items():
                if bundle is None:
                    continue
                ts_sec = bundle.get("ts_sec")
                vol_col = bundle.get("volume")
                open_col = bundle.get("open")
                high_col = bundle.get("high")
                low_col = bundle.get("low")
                close_col = bundle.get("close")
                if ts_sec is None or len(ts_sec) == 0:
                    continue

                mask = (ts_sec >= open_sec) & (ts_sec < day_end_sec)
                if not mask.any():
                    continue

                t_sub = ts_sec[mask]
                v_sub = np.nan_to_num(vol_col[mask], nan=0.0) if vol_col is not None else None
                state = self._quotes_by_sym.get(sym)
                if state is None or state.get("feed_date") not in (None, today):
                    state = {
                        "feed_date": today,
                        "ltp": None,
                        "day_open": None,
                        "day_high": None,
                        "day_low": None,
                        "atp": None,
                        "vtt": None,
                        "last_vtt": None,
                        "last_ts_sec": None,
                        "minute_vols": {},
                    }
                    self._quotes_by_sym[sym] = state

                state["feed_date"] = today
                if v_sub is not None:
                    mvols = state["minute_vols"]
                    for ts_val, vol_val in zip(t_sub, v_sub):
                        m_sec = int(ts_val)
                        mvols[m_sec] = max(float(mvols.get(m_sec, 0.0)), float(vol_val))
                    seeded_sum = float(np.sum(v_sub, dtype=np.float64))
                    if state["vtt"] is None:
                        state["vtt"] = seeded_sum
                        state["last_vtt"] = seeded_sum

                if open_col is not None and state["day_open"] is None:
                    o_sub = open_col[mask]
                    valid_o = o_sub[~np.isnan(o_sub)]
                    if len(valid_o):
                        state["day_open"] = float(valid_o[0])
                if high_col is not None:
                    h_sub = high_col[mask]
                    if not np.isnan(h_sub).all():
                        h_val = float(np.nanmax(h_sub))
                        state["day_high"] = h_val if state["day_high"] is None else max(state["day_high"], h_val)
                if low_col is not None:
                    l_sub = low_col[mask]
                    if not np.isnan(l_sub).all():
                        l_val = float(np.nanmin(l_sub))
                        state["day_low"] = l_val if state["day_low"] is None else min(state["day_low"], l_val)
                if close_col is not None and state["ltp"] is None:
                    c_sub = close_col[mask]
                    valid_c = c_sub[~np.isnan(c_sub)]
                    if len(valid_c):
                        state["ltp"] = float(valid_c[-1])
                if len(t_sub) and state["last_ts_sec"] is None:
                    state["last_ts_sec"] = int(t_sub.max())

            self._seeded_date = today

    def get_status(self) -> Dict[str, Any]:
        with self._state_lock:
            last_age = (pytime.monotonic() - self._last_msg_mono) if self._last_msg_mono > 0 else None
            return {
                "ws_connected": self._connected,
                "ws_started": self._started,
                "subscribed": len(self.key_to_sym),
                "ticks": self._tick_count,
                "symbols_with_ticks": len(self._quotes_by_sym),
                "last_tick_age_s": round(last_age, 1) if last_age is not None else None,
                "error": self._last_error,
            }

    def get_today_summaries(
        self,
        symbols: Iterable[str],
        today: date,
        cutoff: time,
        bar_interval_sec: int = 60,
    ) -> Dict[str, TodaySummary]:
        """Build 8-tuple TodaySummary records for all symbols having valid today live data."""
        day_start_sec = ist_epoch_sec(today, time(0, 0))
        open_sec = day_start_sec + _MARKET_OPEN_SEC
        cutoff_bound_sec = day_start_sec + time_to_sec(cutoff)
        out: Dict[str, TodaySummary] = {}

        with self._state_lock:
            for sym in symbols:
                state = self._quotes_by_sym.get(sym)
                if not state or state.get("feed_date") != today:
                    continue
                last_ts = state.get("last_ts_sec")
                if last_ts is None or last_ts < open_sec:
                    continue
                eff_ts = min(int(last_ts), cutoff_bound_sec)
                rel_sec = eff_ts - day_start_sec
                if rel_sec < _MARKET_OPEN_SEC or rel_sec >= 86400:
                    continue
                last_bar_time = time(rel_sec // 3600, (rel_sec % 3600) // 60, rel_sec % 60)

                vol_today = state.get("vtt")
                if vol_today is None or vol_today <= 0.0:
                    continue
                ltp = state.get("ltp")
                day_open = state.get("day_open") or ltp
                day_high = state.get("day_high") or ltp
                day_low = state.get("day_low") or ltp
                vwap = state.get("atp")
                vol_burst_15m = _compute_burst_from_minute_buckets(
                    state.get("minute_vols") or {},
                    float(vol_today),
                    cutoff_bound_sec,
                    bar_interval_sec,
                )
                out[sym] = (
                    float(vol_today),
                    ltp,
                    day_open,
                    day_high,
                    day_low,
                    last_bar_time,
                    vwap,
                    vol_burst_15m,
                )
        return out


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
    for stream in to_stop:
        stream.stop()


atexit.register(stop_upstox_stream)
