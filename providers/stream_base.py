"""
Shared WebSocket market stream state, rolling 1-minute volume burst computation,
intraday REST seeding, and TodaySummary builder used by broker stream adapters.
"""

from __future__ import annotations

from datetime import date, time
import hashlib
import math
import threading
import time as pytime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np

from providers.base import ArrayBundle, TodaySummary
from utils import ist_epoch_sec, time_to_sec

MARKET_OPEN_SEC = 9 * 3600 + 15 * 60


def to_float(val: Any) -> Optional[float]:
    """Safely coerce numeric or string quote fields to float, returning None for NaN/blanks."""
    if val is None or val == "":
        return None
    try:
        num = float(val)
        return None if math.isnan(num) else num
    except (TypeError, ValueError):
        return None


def secret_fingerprint(*parts: str) -> str:
    """Short deterministic SHA-256 fingerprint for indexing active singleton streams by credential."""
    joined = "|".join(parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()[:16]


def compute_burst_from_minute_buckets(
    minute_vols: Dict[int, float],
    vol_today: float,
    cutoff_bound_sec: int,
    bar_interval_sec: int,
) -> Optional[float]:
    """Compare trailing 15m active-bar mean volume against session active-bar mean."""
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


def new_symbol_quote_state(feed_date: Optional[date] = None) -> Dict[str, Any]:
    """Create a clean per-symbol intraday quote accumulator dictionary."""
    return {
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
        "pv_sum": 0.0,
        "pv_vol": 0.0,
    }


class BaseMarketStream:
    """Thread-safe base accumulator shared across real-time WebSocket stream providers."""

    def __init__(self, key_to_sym: Dict[str, str]) -> None:
        self.key_to_sym: Dict[str, str] = dict(key_to_sym)
        self._state_lock = threading.Lock()
        self._started: bool = False
        self._connected: bool = False
        self._stop_requested: bool = False
        self._last_error: Optional[str] = None
        self._last_msg_mono: float = 0.0
        self._tick_count: int = 0
        self._seeded_date: Optional[date] = None
        self._quotes_by_sym: Dict[str, Dict[str, Any]] = {}

    def _get_or_reset_state_locked(self, sym: str, feed_date: Optional[date]) -> Dict[str, Any]:
        state = self._quotes_by_sym.get(sym)
        if state is None or (feed_date is not None and state.get("feed_date") not in (None, feed_date)):
            state = new_symbol_quote_state(feed_date)
            self._quotes_by_sym[sym] = state
        elif feed_date is not None:
            state["feed_date"] = feed_date
        return state

    def _apply_quote_tick_locked(
        self,
        sym: str,
        feed_date: Optional[date],
        tick_sec: Optional[int],
        ltp: Optional[float],
        day_open: Optional[float],
        day_high: Optional[float],
        day_low: Optional[float],
        atp: Optional[float],
        vtt: Optional[float],
        explicit_minute_bars: Optional[List[Tuple[int, float]]] = None,
    ) -> None:
        """Merge one live tick or snapshot into symbol state, tracking 1m volume buckets and running VWAP."""
        state = self._get_or_reset_state_locked(sym, feed_date)
        if ltp is not None and ltp > 0:
            state["ltp"] = ltp
        if day_open is not None and day_open > 0:
            state["day_open"] = day_open
        if day_high is not None and day_high > 0:
            state["day_high"] = day_high if state["day_high"] is None else max(state["day_high"], day_high)
        if day_low is not None and day_low > 0:
            state["day_low"] = day_low if state["day_low"] is None else min(state["day_low"], day_low)
        if atp is not None and atp > 0:
            state["atp"] = atp
        if tick_sec is not None:
            prev_ts = state["last_ts_sec"]
            state["last_ts_sec"] = tick_sec if prev_ts is None else max(prev_ts, tick_sec)

        minute_vols = state["minute_vols"]
        if explicit_minute_bars:
            for b_sec, b_vol in explicit_minute_bars:
                minute_vols[b_sec] = max(float(minute_vols.get(b_sec, 0.0)), float(b_vol))

        if vtt is not None and vtt >= 0:
            prev_vtt = state["last_vtt"]
            if prev_vtt is not None and vtt > prev_vtt:
                delta_v = vtt - prev_vtt
                if not explicit_minute_bars and tick_sec is not None:
                    m_sec = (tick_sec // 60) * 60
                    minute_vols[m_sec] = minute_vols.get(m_sec, 0.0) + delta_v
                px = ltp if (ltp is not None and ltp > 0) else state.get("ltp")
                if px is not None and px > 0:
                    state["pv_sum"] += float(px) * delta_v
                    state["pv_vol"] += delta_v
            state["vtt"] = vtt if state["vtt"] is None else max(state["vtt"], vtt)
            state["last_vtt"] = state["vtt"]

    def needs_intraday_seed(self, today: date) -> bool:
        with self._state_lock:
            return self._seeded_date != today

    def seed_from_intraday_bundles(self, today: date, bundles: Dict[str, ArrayBundle]) -> None:
        """Seed today's 1-minute volume buckets, open/high/low, and running VWAP from REST intraday bars."""
        day_start_sec = ist_epoch_sec(today, time(0, 0))
        open_sec = day_start_sec + MARKET_OPEN_SEC
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
                state = self._get_or_reset_state_locked(sym, today)

                if v_sub is not None:
                    mvols = state["minute_vols"]
                    for ts_val, vol_val in zip(t_sub, v_sub):
                        m_sec = int(ts_val)
                        mvols[m_sec] = max(float(mvols.get(m_sec, 0.0)), float(vol_val))
                    seeded_sum = float(np.sum(v_sub, dtype=np.float64))
                    if state["vtt"] is None:
                        state["vtt"] = seeded_sum
                        state["last_vtt"] = seeded_sum

                    # Seed running price*volume numerator/denominator so VWAP works even if feed omits ATP
                    c_sub = close_col[mask] if close_col is not None else None
                    if c_sub is not None and state["pv_vol"] == 0.0:
                        if high_col is not None and low_col is not None:
                            tp = (high_col[mask] + low_col[mask] + c_sub) / 3.0
                            nan_tp = np.isnan(tp)
                            if nan_tp.any():
                                tp = np.where(nan_tp, c_sub, tp)
                        else:
                            tp = c_sub
                        valid_pv = (~np.isnan(tp)) & (v_sub > 0.0)
                        if valid_pv.any():
                            pv_v = float(np.sum(v_sub[valid_pv], dtype=np.float64))
                            if pv_v > 0.0:
                                state["pv_sum"] = float(np.sum(tp[valid_pv] * v_sub[valid_pv], dtype=np.float64))
                                state["pv_vol"] = pv_v

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
        open_sec = day_start_sec + MARKET_OPEN_SEC
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
                if rel_sec < MARKET_OPEN_SEC or rel_sec >= 86400:
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
                if vwap is None and state.get("pv_vol", 0.0) > 0.0:
                    vwap = state["pv_sum"] / state["pv_vol"]
                vol_burst_15m = compute_burst_from_minute_buckets(
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
