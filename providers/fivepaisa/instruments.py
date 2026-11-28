"""
5paisa NSE equity ScripMaster resolver & daily disk cache.
Maps scanner symbols (e.g. 'RELIANCE.NS', 'M&M.NS') -> numeric 5paisa ScripCode strings ('2885', '2031').
"""

from __future__ import annotations

import csv
from datetime import datetime
import io
import json
import threading
from typing import Dict, Iterable, Optional, Tuple

from core.utils import CACHE_DIR, IST

try:
    import orjson as _orjson
except ImportError:  # pragma: no cover - optional faster JSON codec
    _orjson = None

FIVEPAISA_SCRIPMASTER_URLS = (
    "https://Openapi.5paisa.com/VendorsAPI/Service1.svc/ScripMaster/segment/nse_eq",
    "https://Openapi.5paisa.com/VendorsAPI/Service1.svc/ScripMaster/segment/All",
    "https://images.5paisa.com/website/scripmaster-csv-format.csv",
)
INSTRUMENTS_CACHE_FILE = CACHE_DIR / "fivepaisa_nse_instruments.json"

# Fallback symbol aliases when static ScripMaster snapshots still carry pre-rename NSE tickers
_LEGACY_SYMBOL_ALIASES: Dict[str, str] = {
    "ACUTAAS": "AMIORG",
    "ETERNAL": "ZOMATO",
    "ICICILIFE": "ICICIPRULI",
    "LTM": "LTIM",
    "TMPV": "TATAMOTORS",
}

_LOCK = threading.Lock()
_MEM_CACHE: Optional[Dict[str, str]] = None
_MEM_CACHE_DATE: Optional[str] = None


def _parse_scripmaster_csv(csv_text: str) -> Dict[str, str]:
    """
    Parse 5paisa ScripMaster CSV into {SYMBOL: scrip_code} for NSE cash equities (Exch=N, ExchType=C),
    strictly preferring Series=='EQ' over non-EQ series (BE/SM/ST/BZ).
    """
    eq_map: Dict[str, str] = {}
    fallback_map: Dict[str, str] = {}
    reader = csv.DictReader(io.StringIO(csv_text.lstrip("\ufeff")))

    for row in reader:
        if not isinstance(row, dict):
            continue
        if str(row.get("Exch") or "").strip().upper() != "N":
            continue
        if str(row.get("ExchType") or "").strip().upper() != "C":
            continue

        code_raw = row.get("Scripcode") if row.get("Scripcode") not in (None, "") else row.get("ScripCode")
        code = str(code_raw or "").strip()
        name = str(row.get("Name") or "").strip().upper()
        if not code or not name or not code.isdigit():
            continue

        series = str(row.get("Series") or "").strip().upper()
        if series == "EQ":
            eq_map[name] = code
        elif series in ("BE", "SM", "ST", "BZ", "") and name not in eq_map and name not in fallback_map:
            fallback_map[name] = code

    if fallback_map:
        for sym, code in fallback_map.items():
            eq_map.setdefault(sym, code)
    return eq_map


def fetch_fivepaisa_nse_eq_map(timeout: int = 30) -> Dict[str, str]:
    """Download 5paisa ScripMaster CSV from live API route or official fallback snapshot."""
    import requests

    last_exc: Optional[Exception] = None
    for url in FIVEPAISA_SCRIPMASTER_URLS:
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            text = resp.content.decode("utf-8-sig", errors="replace").strip()
            if not text or "," not in text[:256]:
                raise ValueError(f"Unexpected non-CSV response from {url}")
            eq_map = _parse_scripmaster_csv(text)
            if not eq_map:
                raise ValueError(f"No NSE cash equity scrips parsed from {url}")
            return eq_map
        except Exception as exc:
            last_exc = exc
            continue
    raise ValueError(f"All 5paisa ScripMaster URLs failed ({last_exc})")


def _load_disk_map() -> Optional[Tuple[str, Dict[str, str]]]:
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


def get_fivepaisa_nse_eq_map(force_refresh: bool = False) -> Dict[str, str]:
    """Return cached or freshly downloaded {SYMBOL: scrip_code} map for today (IST)."""
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
        fresh = fetch_fivepaisa_nse_eq_map()
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


def resolve_scrip_codes(
    symbols: Iterable[str],
    eq_map: Optional[Dict[str, str]] = None,
) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Map scanner symbols (e.g. 'RELIANCE.NS') -> numeric 5paisa ScripCode strings.
    Returns ({scanner_sym: scrip_code}, {scrip_code: scanner_sym}).
    """
    resolved_map = get_fivepaisa_nse_eq_map() if eq_map is None else eq_map
    sym_to_code: Dict[str, str] = {}
    code_to_sym: Dict[str, str] = {}

    for sym in symbols:
        base = sym.removesuffix(".NS").removesuffix(".BO").strip().upper()
        code = resolved_map.get(base)
        if not code and base in _LEGACY_SYMBOL_ALIASES:
            code = resolved_map.get(_LEGACY_SYMBOL_ALIASES[base])
        if code:
            sym_to_code[sym] = str(code)
            code_to_sym[str(code)] = sym
    return sym_to_code, code_to_sym
