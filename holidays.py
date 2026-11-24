"""
Basic NSE holiday / previous trading day helper.
Expand the HOLIDAYS set with official NSE holiday dates for better accuracy.
"""

from datetime import datetime, timedelta, date
import pytz

IST = pytz.timezone("Asia/Kolkata")

# Major NSE holidays 2025-2026 (add more as needed)
HOLIDAYS = {
    date(2025, 1, 26),   # Republic Day
    date(2025, 3, 14),   # Holi
    date(2025, 3, 31),   # Id-ul-Fitr (approx)
    date(2025, 4, 10),   # Mahavir Jayanti
    date(2025, 4, 14),   # Dr. Ambedkar Jayanti / Good Friday
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


def get_previous_trading_day(ref_date: date | None = None) -> date:
    """Return the most recent trading day before ref_date (or today)."""
    if ref_date is None:
        ref_date = datetime.now(IST).date()

    d = ref_date - timedelta(days=1)
    while d.weekday() >= 5 or d in HOLIDAYS:  # Sat=5, Sun=6
        d -= timedelta(days=1)
    return d


def is_market_open(now: datetime | None = None) -> bool:
    """Rough check: Mon-Fri, 09:15 - 15:30 IST (ignores holidays for simplicity)."""
    if now is None:
        now = datetime.now(IST)
    if now.weekday() >= 5:
        return False
    t = now.time()
    return (t >= datetime.strptime("09:15", "%H:%M").time() and
            t <= datetime.strptime("15:30", "%H:%M").time())
