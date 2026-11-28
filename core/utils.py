"""
Shared timezone, JSON decoding, NSE date parsing, and HTTP helpers.
"""

from datetime import date, datetime, time
from pathlib import Path
from typing import Any, Optional
import pytz

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = PROJECT_ROOT / ".cache"

IST = pytz.timezone("Asia/Kolkata")

try:
    import orjson as _orjson
except ImportError:  # pragma: no cover - optional faster JSON decoder
    _orjson = None

_UNSET = object()


def localize_ist(dt_naive: datetime) -> datetime:
    """Attach IST timezone to a naive datetime (supports pytz and stdlib tzinfo)."""
    localize = getattr(IST, "localize", None)
    return localize(dt_naive) if localize is not None else dt_naive.replace(tzinfo=IST)


def time_to_sec(t: time) -> int:
    """Convert wall-clock time to seconds since midnight."""
    return t.hour * 3600 + t.minute * 60 + t.second


def ist_epoch_sec(d: date, t: time) -> int:
    """Convert an IST wall-clock (date, time) pair to UTC integer epoch seconds."""
    return int(localize_ist(datetime.combine(d, t)).timestamp())


def decode_json_response(resp: Any, orjson_mod: Any = _UNSET) -> Any:
    """Decode HTTP JSON response via orjson when raw bytes are present, else resp.json()."""
    decoder = _orjson if orjson_mod is _UNSET else orjson_mod
    raw = getattr(resp, "content", None)
    if decoder is not None and isinstance(raw, (bytes, bytearray, memoryview)) and raw:
        return decoder.loads(raw)
    return resp.json()


def parse_nse_date(raw: Optional[str]) -> Optional[date]:
    """Parse common NSE date strings into a date object."""
    text = (raw or "").strip()
    if not text:
        return None
    for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def parse_nse_ts(raw: Optional[str]) -> Optional[datetime]:
    """Parse common NSE timestamp strings into an IST-aware datetime."""
    text = (raw or "").strip()
    if not text:
        return None
    for fmt in ("%d-%b-%Y %H:%M:%S", "%d-%B-%Y %H:%M:%S", "%d-%b-%Y %H:%M"):
        try:
            return localize_ist(datetime.strptime(text, fmt))
        except ValueError:
            continue
    return None


def fetch_nse_json(warmup_url: str, api_url: str, timeout: int = 10) -> Any:
    """Open an ephemeral browser-impersonated session, warm cookies on NSE, and fetch JSON."""
    from curl_cffi import requests as curl_requests

    with curl_requests.Session(impersonate="chrome") as session:
        session.get(warmup_url, timeout=timeout)
        resp = session.get(api_url, timeout=timeout)
        resp.raise_for_status()
        return decode_json_response(resp)
