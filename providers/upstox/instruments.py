"""
Upstox NSE equity instrument master resolver & daily disk cache.
Maps scanner symbols (e.g. 'RELIANCE.NS' / 'M&M.NS') -> Upstox instrument_key ('NSE_EQ|INE...').
"""

from datetime import datetime
import gzip
import json
import threading
from typing import Dict, Iterable, Optional

from core.utils import CACHE_DIR, IST

try:
    import orjson as _orjson
except ImportError:  # pragma: no cover - optional faster JSON parser
    _orjson = None

UPSTOX_NSE_INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
INSTRUMENTS_CACHE_FILE = CACHE_DIR / "upstox_nse_instruments.json"

_LOCK = threading.Lock()
_MEM_CACHE: Optional[Dict[str, str]] = None
_MEM_CACHE_DATE: Optional[str] = None


def _parse_instruments_payload(items: Iterable[dict]) -> Dict[str, str]:
    """Build {trading_symbol: instrument_key} for NSE_EQ equities, strictly preferring instrument_type=='EQ'."""
    eq_map: Dict[str, str] = {}
    fallback_map: Dict[str, str] = {}

    for item in items:
        if not isinstance(item, dict):
            continue
        if item.get("segment") != "NSE_EQ":
            continue
        tsym = str(item.get("trading_symbol") or "").strip().upper()
        ikey = str(item.get("instrument_key") or "").strip()
        if not tsym or not ikey:
            continue
        itype = str(item.get("instrument_type") or "").strip().upper()
        if itype == "EQ":
            eq_map[tsym] = ikey
        elif itype in ("BE", "SM", "ST", "BZ") and tsym not in eq_map and tsym not in fallback_map:
            fallback_map[tsym] = ikey

    if fallback_map:
        for sym, key in fallback_map.items():
            eq_map.setdefault(sym, key)
    return eq_map


def fetch_upstox_nse_eq_map(timeout: int = 25) -> Dict[str, str]:
    """Download and decompress Upstox's NSE.json.gz instrument master into an equity key map."""
    import requests

    resp = requests.get(UPSTOX_NSE_INSTRUMENTS_URL, timeout=timeout)
    resp.raise_for_status()
    raw_bytes = gzip.decompress(resp.content)
    items = _orjson.loads(raw_bytes) if _orjson is not None else json.loads(raw_bytes.decode("utf-8"))
    if not isinstance(items, list) or not items:
        raise ValueError("Empty Upstox NSE instrument payload")
    eq_map = _parse_instruments_payload(items)
    if not eq_map:
        raise ValueError("No NSE_EQ equity instruments found in Upstox NSE master")
    return eq_map


def _load_disk_map() -> Optional[tuple[str, Dict[str, str]]]:
    if not INSTRUMENTS_CACHE_FILE.exists():
        return None
    try:
        raw = INSTRUMENTS_CACHE_FILE.read_bytes()
        payload = _orjson.loads(raw) if _orjson is not None else json.loads(raw.decode("utf-8"))
        cached_date = str(payload.get("date") or "")
        mapping = payload.get("eq_by_symbol")
        if isinstance(mapping, dict) and mapping:
            return cached_date, {str(k): str(v) for k, v in mapping.items()}
    except Exception:
        pass
    return None


def _save_disk_map(date_str: str, mapping: Dict[str, str]) -> None:
    try:
        INSTRUMENTS_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        payload = {"date": date_str, "eq_by_symbol": mapping}
        if _orjson is not None:
            INSTRUMENTS_CACHE_FILE.write_bytes(_orjson.dumps(payload))
        else:
            INSTRUMENTS_CACHE_FILE.write_text(json.dumps(payload), encoding="utf-8")
    except Exception:
        pass


def get_upstox_nse_eq_map(force_refresh: bool = False) -> Dict[str, str]:
    """Return cached or freshly downloaded {SYMBOL: NSE_EQ|INE...} map for today (IST)."""
    global _MEM_CACHE, _MEM_CACHE_DATE
    today_str = datetime.now(IST).date().isoformat()

    with _LOCK:
        if not force_refresh and _MEM_CACHE is not None and _MEM_CACHE_DATE == today_str:
            return dict(_MEM_CACHE)

    if not force_refresh:
        disk = _load_disk_map()
        if disk is not None and disk[0] == today_str:
            with _LOCK:
                _MEM_CACHE_DATE, _MEM_CACHE = disk[0], disk[1]
            return dict(disk[1])

    try:
        fresh = fetch_upstox_nse_eq_map()
        _save_disk_map(today_str, fresh)
        with _LOCK:
            _MEM_CACHE_DATE, _MEM_CACHE = today_str, fresh
        return dict(fresh)
    except Exception:
        disk = _load_disk_map()
        if disk is not None:
            with _LOCK:
                _MEM_CACHE_DATE, _MEM_CACHE = disk[0], disk[1]
            return dict(disk[1])
        raise


def resolve_instrument_keys(
    symbols: Iterable[str],
    eq_map: Optional[Dict[str, str]] = None,
) -> tuple[Dict[str, str], Dict[str, str]]:
    """
    Map scanner symbols (e.g. 'RELIANCE.NS') -> Upstox instrument_key.
    Returns ({scanner_sym: instrument_key}, {instrument_key: scanner_sym}).
    """
    resolved_map = get_upstox_nse_eq_map() if eq_map is None else eq_map
    sym_to_key: Dict[str, str] = {}
    key_to_sym: Dict[str, str] = {}
    for sym in symbols:
        base = sym.removesuffix(".NS").removesuffix(".BO").strip().upper()
        ikey = resolved_map.get(base)
        if ikey:
            sym_to_key[sym] = ikey
            key_to_sym[ikey] = sym
    return sym_to_key, key_to_sym
