"""
NSE holiday calendar and previous trading day helper.
Combines:
1. Live NSE official trading holiday sync (cached in .cache/nse_holidays.json) with built-in fallback.
2. Empirical recent trading-day detection via ^NSEI daily bars (handles unscheduled closures & special sessions).
"""

from datetime import datetime, timedelta, date
import json
from pathlib import Path
import threading
import time as pytime
from typing import Any, Dict, Optional, Set, Tuple

from utils import IST, decode_json_response, fetch_nse_json, parse_nse_date as _parse_nse_date

CACHE_FILE = Path(__file__).parent / ".cache" / "nse_holidays.json"

# Built-in fallback NSE holidays 2025-2026
HOLIDAYS: Set[date] = {
    date(2025, 1, 26),   # Republic Day
    date(2025, 3, 14),   # Holi
    date(2025, 3, 31),   # Id-ul-Fitr (approx)
    date(2025, 4, 10),   # Mahavir Jayanti
    date(2025, 4, 14),   # Dr. Ambedkar Jayanti
    date(2025, 4, 18),   # Good Friday
    date(2025, 5, 1),    # Maharashtra Day
    date(2025, 8, 15),   # Independence Day
    date(2025, 8, 27),   # Ganesh Chaturthi (approx)
    date(2025, 10, 2),   # Gandhi Jayanti
    date(2025, 10, 21),  # Diwali (approx)
    date(2025, 10, 22),  # Diwali Laxmi Pujan (approx)
    date(2025, 11, 5),   # Guru Nanak Jayanti (approx)
    date(2025, 12, 25),  # Christmas
    # 2026
    date(2026, 1, 26),
    date(2026, 3, 3),    # Holi (approx)
    date(2026, 3, 21),   # Id-ul-Fitr (approx)
    date(2026, 4, 3),    # Good Friday (approx)
    date(2026, 4, 14),
    date(2026, 5, 1),
    date(2026, 8, 15),
    date(2026, 10, 2),
    date(2026, 10, 20),  # Diwali (approx)
    date(2026, 11, 24),  # Guru Nanak (approx)
    date(2026, 12, 25),
}

_STATE_LOCK = threading.Lock()
_NSEI_HTTP_LOCK = threading.Lock()
_HOLIDAYS_MEMORY: Optional[Tuple[date, Set[date]]] = None
_NSE_HOLIDAY_COOLDOWN_UNTIL: float = 0.0
_HOLIDAY_REFRESH_INFLIGHT: bool = False
_NSEI_SESSION: Any = None
_NSEI_CACHE: Dict[str, Tuple[date, float, Optional[Set[date]]]] = {}

_RETRY_COOLDOWN_SECONDS = 900.0  # 15 minutes
_NSEI_TTL_SECONDS = 900.0
_HOLIDAY_DISK_MAX_AGE_DAYS = 7


def fetch_nse_holidays(timeout: int = 4) -> Set[date]:
    """Fetch official NSE trading holidays from NSE India API."""
    payload = fetch_nse_json(
        warmup_url="https://www.nseindia.com/resources/exchange-communication-holidays",
        api_url="https://www.nseindia.com/api/holiday-master?type=trading",
        timeout=timeout,
    )
    if not isinstance(payload, dict):
        raise ValueError("Unexpected NSE holiday-master payload type")

    # Prefer Equity Derivatives (FO) or Capital Market (CM), then fall back to any segment list
    items = payload.get("FO") or payload.get("CM")
    if not items:
        items = []
        for seg_items in payload.values():
            if isinstance(seg_items, list):
                items.extend(seg_items)

    parsed: Set[date] = set()
    for item in items:
        if isinstance(item, dict) and item.get("tradingDate"):
            dt = _parse_nse_date(str(item["tradingDate"]))
            if dt is not None:
                parsed.add(dt)

    if not parsed:
        raise ValueError("Empty holiday list returned from NSE")
    return parsed


def _load_disk_holidays() -> Optional[Tuple[date, Set[date]]]:
    if not CACHE_FILE.exists():
        return None
    try:
        payload = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        synced_str = payload.get("date")
        raw_dates = payload.get("holidays") or []
        synced_date = _parse_nse_date(str(synced_str)) if synced_str else None
        dates = {d for d in (_parse_nse_date(str(x)) for x in raw_dates) if d is not None}
        if dates and synced_date:
            return synced_date, dates
    except Exception:
        pass
    return None


def _save_disk_holidays(synced_date: date, holidays_set: Set[date]) -> None:
    try:
        CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        CACHE_FILE.write_text(
            json.dumps(
                {
                    "date": synced_date.isoformat(),
                    "holidays": sorted(d.isoformat() for d in holidays_set),
                }
            ),
            encoding="utf-8",
        )
    except Exception:
        pass


def _fetch_and_store_holidays(today: date) -> Optional[Set[date]]:
    global _HOLIDAYS_MEMORY, _NSE_HOLIDAY_COOLDOWN_UNTIL
    try:
        live_set = fetch_nse_holidays()
        # Keep built-in historical holidays from prior years while trusting NSE for fetched years
        live_years = {d.year for d in live_set}
        merged = live_set | {d for d in HOLIDAYS if d.year not in live_years}
        _save_disk_holidays(today, merged)
        with _STATE_LOCK:
            _HOLIDAYS_MEMORY = (today, merged)
            _NSE_HOLIDAY_COOLDOWN_UNTIL = 0.0
        return merged
    except Exception:
        with _STATE_LOCK:
            _NSE_HOLIDAY_COOLDOWN_UNTIL = pytime.monotonic() + _RETRY_COOLDOWN_SECONDS
        return None


def _spawn_holiday_refresh_async(today: date) -> None:
    global _HOLIDAY_REFRESH_INFLIGHT
    with _STATE_LOCK:
        if _HOLIDAY_REFRESH_INFLIGHT:
            return
        _HOLIDAY_REFRESH_INFLIGHT = True

    def _worker() -> None:
        global _HOLIDAY_REFRESH_INFLIGHT
        try:
            _fetch_and_store_holidays(today)
        finally:
            with _STATE_LOCK:
                _HOLIDAY_REFRESH_INFLIGHT = False

    threading.Thread(target=_worker, name="nse_holiday_refresh", daemon=True).start()


def get_holidays(
    force_refresh: bool = False,
    background_refresh: bool = False,
) -> Set[date]:
    """
    Return NSE trading holidays combining cached/live NSE calendar with built-in fallback.
    Refreshes from NSE lazily when stale (>7 days) or missing the current year.
    When background_refresh=True (and not force_refresh), returns cached/built-in data
    immediately while refreshing stale holidays asynchronously in the background.
    """
    global _HOLIDAYS_MEMORY

    today = datetime.now(IST).date()
    now_mono = pytime.monotonic()

    with _STATE_LOCK:
        if not force_refresh and _HOLIDAYS_MEMORY is not None:
            synced_date, cached_set = _HOLIDAYS_MEMORY
            is_fresh = (today - synced_date).days <= _HOLIDAY_DISK_MAX_AGE_DAYS and any(
                d.year == today.year for d in cached_set
            )
            if is_fresh or _HOLIDAY_REFRESH_INFLIGHT or now_mono < _NSE_HOLIDAY_COOLDOWN_UNTIL:
                return set(cached_set)

        disk_loaded = _load_disk_holidays() if not force_refresh else None
        if disk_loaded is not None:
            synced_date, disk_set = disk_loaded
            if (today - synced_date).days <= _HOLIDAY_DISK_MAX_AGE_DAYS and any(
                d.year == today.year for d in disk_set
            ):
                merged = set(disk_set)
                _HOLIDAYS_MEMORY = (synced_date, merged)
                return set(merged)

        should_fetch = force_refresh or (not _HOLIDAY_REFRESH_INFLIGHT and now_mono >= _NSE_HOLIDAY_COOLDOWN_UNTIL)

    if should_fetch:
        if background_refresh and not force_refresh:
            _spawn_holiday_refresh_async(today)
        else:
            fetched = _fetch_and_store_holidays(today)
            if fetched is not None:
                return set(fetched)

    # Preserve original disk sync date so stale caches still retry after the cooldown window.
    if disk_loaded is None and force_refresh:
        disk_loaded = _load_disk_holidays()
    if disk_loaded is not None:
        disk_synced_date, disk_set = disk_loaded
        merged_disk = set(disk_set)
        with _STATE_LOCK:
            _HOLIDAYS_MEMORY = (disk_synced_date, merged_disk)
        return set(merged_disk)

    with _STATE_LOCK:
        if _HOLIDAYS_MEMORY is not None:
            return set(_HOLIDAYS_MEMORY[1])
    return set(HOLIDAYS)


def _fetch_nsei_dates_direct(
    period: str = "15d",
    interval: str = "1d",
    timeout: int = 3,
) -> Optional[Set[date]]:
    """Fast Yahoo Chart API path for ^NSEI trading dates without pandas/yfinance overhead."""
    global _NSEI_SESSION
    try:
        from curl_cffi import requests as curl_requests
    except ImportError:
        return None

    try:
        with _NSEI_HTTP_LOCK:
            if _NSEI_SESSION is None:
                _NSEI_SESSION = curl_requests.Session(impersonate="chrome")
            resp = _NSEI_SESSION.get(
                "https://query1.finance.yahoo.com/v8/finance/chart/%5ENSEI",
                params={"interval": interval, "range": period, "includePrePost": "false"},
                timeout=timeout,
            )
            if resp.status_code != 200:
                return None
            payload = decode_json_response(resp)
    except Exception:
        with _NSEI_HTTP_LOCK:
            if _NSEI_SESSION is not None:
                try:
                    _NSEI_SESSION.close()
                except Exception:
                    pass
                _NSEI_SESSION = None
        return None

    try:
        res0 = (payload or {}).get("chart", {}).get("result", [None])[0]
        if not isinstance(res0, dict):
            return None
        timestamps = res0.get("timestamp") or []
        quotes = res0.get("indicators", {}).get("quote") or []
        closes = quotes[0].get("close") if quotes and isinstance(quotes[0], dict) else None
        dates: Set[date] = set()
        for idx, ts in enumerate(timestamps):
            if ts is None:
                continue
            if closes is not None and idx < len(closes) and closes[idx] is None:
                continue
            dates.add(datetime.fromtimestamp(int(ts), IST).date())
        return dates if dates else None
    except Exception:
        return None


def _fetch_nsei_trading_dates(
    period: str = "15d",
    interval: str = "1d",
    timeout: int = 3,
) -> Optional[Set[date]]:
    """Fetch recent Nifty 50 (^NSEI) bar dates to detect actual trading sessions."""
    direct_dates = _fetch_nsei_dates_direct(period=period, interval=interval, timeout=timeout)
    if direct_dates:
        return direct_dates

    try:
        import yfinance as yf

        tkr = yf.Ticker("^NSEI")
        if getattr(tkr, "_tz", None) is None:
            tkr._tz = "Asia/Kolkata"
        df = tkr.history(period=period, interval=interval, prepost=False, actions=False, timeout=timeout)
        if df is None or df.empty:
            return None
        if "Close" in df.columns:
            df = df.dropna(subset=["Close"])
            if df.empty:
                return None
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC").tz_convert(IST)
        elif str(df.index.tz) != "Asia/Kolkata":
            df.index = df.index.tz_convert(IST)
        dates = set(df.index.date)
        return dates if dates else None
    except Exception:
        return None


def _get_recent_nsei_trading_dates(
    today_ist: date,
    interval: str = "1d",
) -> Optional[Set[date]]:
    """Cached empirical ^NSEI trading dates keyed by interval and IST date with 15-minute TTL."""
    now_mono = pytime.monotonic()
    with _STATE_LOCK:
        cached = _NSEI_CACHE.get(interval)
        if cached is not None:
            cached_for_date, expires_at, cached_dates = cached
            if cached_for_date == today_ist and now_mono < expires_at:
                return set(cached_dates) if cached_dates is not None else None

    dates = _fetch_nsei_trading_dates(period="15d", interval=interval)
    with _STATE_LOCK:
        ttl = _NSEI_TTL_SECONDS if dates else _RETRY_COOLDOWN_SECONDS
        _NSEI_CACHE[interval] = (today_ist, pytime.monotonic() + ttl, dates)
        return set(dates) if dates is not None else None


def _confirm_calendar_prev_via_intraday(
    today_ist: date,
    cal_prev: date,
    emp_prev: date,
) -> date:
    """
    Resolve emp_prev < cal_prev by checking ^NSEI hourly bars:
    - If cal_prev has intraday bars, Yahoo 1d lagged/dropped a bar -> return cal_prev.
    - If hourly bars confirm an earlier session (<= emp_prev) without cal_prev -> return emp_prev.
    - If hourly verification is inconclusive/unavailable -> fall back safely to cal_prev.
    """
    hourly_dates = _get_recent_nsei_trading_dates(today_ist, interval="1h")
    if not hourly_dates:
        return cal_prev

    if cal_prev in hourly_dates:
        with _STATE_LOCK:
            cached_1d = _NSEI_CACHE.get("1d")
            if cached_1d is not None and cached_1d[0] == today_ist and cached_1d[2] is not None:
                _NSEI_CACHE["1d"] = (today_ist, cached_1d[1], set(cached_1d[2]) | hourly_dates)
        return cal_prev

    if any(d <= emp_prev for d in hourly_dates):
        return emp_prev
    return cal_prev


def get_previous_trading_day(
    ref_date: date | None = None,
    use_market_data: bool = True,
) -> date:
    """
    Return the most recent trading day before ref_date (or today).
    Uses the NSE holiday calendar as baseline, and for today checks recent ^NSEI sessions
    to catch special weekend sessions or verified unscheduled closures without letting
    Yahoo 1d bar lag skip a real trading day.
    """
    today_ist = datetime.now(IST).date()
    if ref_date is None:
        ref_date = today_ist

    holidays = get_holidays()
    cal_prev = ref_date - timedelta(days=1)
    while cal_prev.weekday() >= 5 or cal_prev in holidays:  # Sat=5, Sun=6
        cal_prev -= timedelta(days=1)

    if use_market_data and ref_date == today_ist:
        nsei_dates = _get_recent_nsei_trading_dates(today_ist)
        if nsei_dates:
            past_dates = [d for d in nsei_dates if d < ref_date]
            if past_dates:
                emp_prev = max(past_dates)
                if (ref_date - emp_prev).days <= 14:
                    if emp_prev >= cal_prev:
                        return emp_prev
                    return _confirm_calendar_prev_via_intraday(today_ist, cal_prev, emp_prev)

    return cal_prev


def is_market_open(now: datetime | None = None, background_refresh: bool = False) -> bool:
    """Check whether NSE equity market is open: Mon-Fri (excluding holidays), 09:15 - 15:30 IST."""
    if now is None:
        now = datetime.now(IST)
    holidays = get_holidays(background_refresh=background_refresh)
    if now.weekday() >= 5 or now.date() in holidays:
        return False
    t = now.time()
    return (t >= datetime.strptime("09:15", "%H:%M").time() and
            t <= datetime.strptime("15:30", "%H:%M").time())
