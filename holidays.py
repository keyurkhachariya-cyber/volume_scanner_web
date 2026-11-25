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
from typing import Optional, Set, Tuple
import pytz

IST = pytz.timezone("Asia/Kolkata")
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
_HOLIDAYS_MEMORY: Optional[Tuple[date, Set[date]]] = None
_NSE_HOLIDAY_COOLDOWN_UNTIL: float = 0.0
_NSEI_CACHE: Optional[Tuple[float, Optional[Set[date]]]] = None

_RETRY_COOLDOWN_SECONDS = 900.0  # 15 minutes
_NSEI_TTL_SECONDS = 900.0
_HOLIDAY_DISK_MAX_AGE_DAYS = 7


def _parse_nse_date(raw: str) -> Optional[date]:
    text = (raw or "").strip()
    for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def fetch_nse_holidays(timeout: int = 4) -> Set[date]:
    """Fetch official NSE trading holidays from NSE India API."""
    from curl_cffi import requests

    session = requests.Session(impersonate="chrome")
    session.get("https://www.nseindia.com/resources/exchange-communication-holidays", timeout=timeout)
    resp = session.get("https://www.nseindia.com/api/holiday-master?type=trading", timeout=timeout)
    resp.raise_for_status()

    payload = resp.json()
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


def get_holidays(force_refresh: bool = False) -> Set[date]:
    """
    Return NSE trading holidays combining cached/live NSE calendar with built-in fallback.
    Refreshes from NSE lazily when stale (>7 days) or missing the current year.
    """
    global _HOLIDAYS_MEMORY, _NSE_HOLIDAY_COOLDOWN_UNTIL

    today = datetime.now(IST).date()
    now_mono = pytime.monotonic()

    with _STATE_LOCK:
        if not force_refresh and _HOLIDAYS_MEMORY is not None:
            synced_date, cached_set = _HOLIDAYS_MEMORY
            if (today - synced_date).days <= _HOLIDAY_DISK_MAX_AGE_DAYS and any(
                d.year == today.year for d in cached_set
            ):
                return cached_set

        disk_loaded = _load_disk_holidays() if not force_refresh else None
        if disk_loaded is not None:
            synced_date, disk_set = disk_loaded
            if (today - synced_date).days <= _HOLIDAY_DISK_MAX_AGE_DAYS and any(
                d.year == today.year for d in disk_set
            ):
                merged = set(disk_set)
                _HOLIDAYS_MEMORY = (synced_date, merged)
                return merged

        should_fetch = force_refresh or (now_mono >= _NSE_HOLIDAY_COOLDOWN_UNTIL)

    if should_fetch:
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

    # Fall back to disk cache (even if slightly stale) or built-in HOLIDAYS
    if disk_loaded is not None:
        _, disk_set = disk_loaded
        with _STATE_LOCK:
            _HOLIDAYS_MEMORY = (today, disk_set)
        return disk_set

    with _STATE_LOCK:
        if _HOLIDAYS_MEMORY is not None:
            return _HOLIDAYS_MEMORY[1]
    return set(HOLIDAYS)


def _fetch_nsei_trading_dates(timeout: int = 3) -> Optional[Set[date]]:
    """Fetch recent Nifty 50 (^NSEI) daily bar dates to detect actual trading sessions."""
    try:
        import yfinance as yf

        tkr = yf.Ticker("^NSEI")
        if getattr(tkr, "_tz", None) is None:
            tkr._tz = "Asia/Kolkata"
        df = tkr.history(period="15d", interval="1d", prepost=False, actions=False, timeout=timeout)
        if df is None or df.empty:
            return None
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC").tz_convert(IST)
        elif str(df.index.tz) != "Asia/Kolkata":
            df.index = df.index.tz_convert(IST)
        dates = set(df.index.date)
        return dates if dates else None
    except Exception:
        return None


def _get_recent_nsei_trading_dates() -> Optional[Set[date]]:
    """Cached empirical ^NSEI trading dates with 15-minute TTL and failure cooldown."""
    global _NSEI_CACHE
    now_mono = pytime.monotonic()
    with _STATE_LOCK:
        if _NSEI_CACHE is not None:
            expires_at, cached_dates = _NSEI_CACHE
            if now_mono < expires_at:
                return cached_dates

    dates = _fetch_nsei_trading_dates()
    with _STATE_LOCK:
        ttl = _NSEI_TTL_SECONDS if dates else _RETRY_COOLDOWN_SECONDS
        _NSEI_CACHE = (pytime.monotonic() + ttl, dates)
    return dates


def get_previous_trading_day(
    ref_date: date | None = None,
    use_market_data: bool = True,
) -> date:
    """
    Return the most recent trading day before ref_date (or today).
    For the live current date, empirically checks recent ^NSEI trading sessions first
    (handling unscheduled closures and special sessions) and falls back to the NSE holiday calendar.
    """
    today_ist = datetime.now(IST).date()
    if ref_date is None:
        ref_date = today_ist

    if use_market_data and ref_date == today_ist:
        nsei_dates = _get_recent_nsei_trading_dates()
        if nsei_dates:
            past_dates = [d for d in nsei_dates if d < ref_date]
            if past_dates:
                emp_prev = max(past_dates)
                if (ref_date - emp_prev).days <= 14:
                    return emp_prev

    holidays = get_holidays()
    d = ref_date - timedelta(days=1)
    while d.weekday() >= 5 or d in holidays:  # Sat=5, Sun=6
        d -= timedelta(days=1)
    return d


def is_market_open(now: datetime | None = None) -> bool:
    """Check whether NSE equity market is open: Mon-Fri (excluding holidays), 09:15 - 15:30 IST."""
    if now is None:
        now = datetime.now(IST)
    holidays = get_holidays()
    if now.weekday() >= 5 or now.date() in holidays:
        return False
    t = now.time()
    return (t >= datetime.strptime("09:15", "%H:%M").time() and
            t <= datetime.strptime("15:30", "%H:%M").time())
