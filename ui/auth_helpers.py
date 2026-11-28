"""
Sidebar broker auth helpers:
- OAuth callback URL/parameter extraction
- Short-lived single-use OAuth state handoff (preserves app credentials across browser redirects)
- Local same-day disk persistence (.cache/broker_tokens.json) for localhost/LAN sessions
- 5paisa JWT claim & expiry inspection
"""

from __future__ import annotations

from datetime import date, datetime
import ipaddress
import json
import os
from pathlib import Path
import secrets
import tempfile
import threading
import time as pytime
from typing import Any, Dict, Optional, Sequence, Tuple
from urllib.parse import parse_qs, urlparse

from core.utils import CACHE_DIR, IST
from providers.fivepaisa.stream import decode_jwt_claims, extract_client_code_from_jwt

LOCAL_TOKENS_FILE: Path = CACHE_DIR / "broker_tokens.json"

_HANDOFF_LOCK = threading.Lock()
_PENDING_OAUTH_HANDOFFS: Dict[str, Tuple[float, Dict[str, str]]] = {}
_MAX_PENDING_HANDOFFS = 64

_LOCAL_STORE_LOCK = threading.Lock()
_TOKEN_KEEP_FIELDS: Dict[str, Tuple[str, ...]] = {
    "upstox": ("access_token", "client_id", "client_secret", "redirect_uri"),
    "fivepaisa": (
        "access_token",
        "client_code",
        "user_key",
        "encryption_key",
        "user_id",
        "redirect_uri",
    ),
}


def extract_oauth_param(raw_value: str, param_names: Sequence[str]) -> str:
    """
    Extract an OAuth authorization code or RequestToken from either a full callback URL
    (e.g. 'http://localhost:8501/?code=abc&state=1'), a bare query string, or return the stripped token directly.
    """
    text = (raw_value or "").strip()
    if not text:
        return ""

    query = ""
    if "://" in text or text.startswith(("?", "/")):
        query = urlparse(text).query
    elif "=" in text and any(f"{name.lower()}=" in text.lower() for name in param_names):
        query = text.lstrip("?")

    if query:
        parsed = parse_qs(query, keep_blank_values=False)
        lowered = {k.lower(): v for k, v in parsed.items()}
        for name in param_names:
            vals = parsed.get(name) or lowered.get(name.lower())
            if vals and str(vals[0]).strip():
                return str(vals[0]).strip()
    return text


def is_local_host_request(headers: Any) -> bool:
    """Return True only when request Host indicates localhost, loopback, or private LAN."""
    if not headers or not hasattr(headers, "get"):
        return False
    try:
        raw_host = headers.get("Host") or headers.get("host") or ""
    except Exception:
        return False
    if not isinstance(raw_host, str) or not raw_host.strip():
        return False

    host = raw_host.strip().lower()
    if host.startswith("["):
        host = host[1:].split("]", 1)[0]
    else:
        host = host.split(":", 1)[0]

    if host in ("localhost", "localhost.localdomain"):
        return True
    try:
        ip = ipaddress.ip_address(host)
        return ip.is_loopback or ip.is_private
    except ValueError:
        return False


def detect_default_redirect_uri(
    context_url: Any = None,
    headers: Any = None,
    fallback: str = "http://localhost:8501",
) -> str:
    """
    Detect the active app root URL from st.context.url or HTTP Host headers so Streamlit Cloud
    deployments default to https://<app>.streamlit.app instead of http://localhost:8501.
    """
    if isinstance(context_url, str) and context_url.strip():
        parsed = urlparse(context_url.strip())
        if parsed.scheme in ("http", "https") and parsed.netloc:
            path = (parsed.path or "").rstrip("/")
            return f"{parsed.scheme}://{parsed.netloc}{path}"

    if headers and hasattr(headers, "get"):
        try:
            raw_host = headers.get("Host") or headers.get("host") or ""
        except Exception:
            raw_host = ""
        if isinstance(raw_host, str) and raw_host.strip():
            try:
                raw_proto = headers.get("X-Forwarded-Proto") or headers.get("x-forwarded-proto") or ""
            except Exception:
                raw_proto = ""
            proto = (
                str(raw_proto).split(",")[0].strip().lower()
                if isinstance(raw_proto, str)
                else ""
            )
            if proto not in ("http", "https"):
                proto = "http" if is_local_host_request(headers) else "https"
            return f"{proto}://{raw_host.strip()}"

    return fallback


def _prune_handoffs_locked(now_mono: float) -> None:
    expired = [k for k, (exp, _) in _PENDING_OAUTH_HANDOFFS.items() if exp <= now_mono]
    for k in expired:
        _PENDING_OAUTH_HANDOFFS.pop(k, None)
    while len(_PENDING_OAUTH_HANDOFFS) >= _MAX_PENDING_HANDOFFS:
        oldest_key = min(_PENDING_OAUTH_HANDOFFS, key=lambda k: _PENDING_OAUTH_HANDOFFS[k][0])
        _PENDING_OAUTH_HANDOFFS.pop(oldest_key, None)


def register_oauth_handoff(
    payload: Dict[str, str],
    nonce: str = "",
    ttl_seconds: float = 600.0,
) -> str:
    """Register or refresh an opaque single-use OAuth state handoff mapping."""
    clean_nonce = (nonce or "").strip() or secrets.token_urlsafe(18)
    clean_payload = {str(k): str(v or "").strip() for k, v in payload.items()}
    now_mono = pytime.monotonic()
    with _HANDOFF_LOCK:
        _prune_handoffs_locked(now_mono)
        _PENDING_OAUTH_HANDOFFS[clean_nonce] = (now_mono + max(1.0, float(ttl_seconds)), clean_payload)
    return clean_nonce


def pop_oauth_handoff(nonce: str) -> Optional[Dict[str, str]]:
    """Consume and return a pending OAuth state handoff payload if present and unexpired."""
    clean_nonce = (nonce or "").strip()
    if not clean_nonce:
        return None
    now_mono = pytime.monotonic()
    with _HANDOFF_LOCK:
        entry = _PENDING_OAUTH_HANDOFFS.pop(clean_nonce, None)
        _prune_handoffs_locked(now_mono)
    if entry is None:
        return None
    exp_mono, payload = entry
    return dict(payload) if exp_mono > now_mono else None


def _read_local_store_unlocked() -> Dict[str, Any]:
    if not LOCAL_TOKENS_FILE.exists():
        return {}
    try:
        raw = LOCAL_TOKENS_FILE.read_text(encoding="utf-8")
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_local_store_unlocked(data: Dict[str, Any]) -> None:
    tmp_path: Optional[Path] = None
    try:
        LOCAL_TOKENS_FILE.parent.mkdir(parents=True, exist_ok=True)
        fd, raw_tmp = tempfile.mkstemp(
            dir=str(LOCAL_TOKENS_FILE.parent),
            prefix=".tmp_broker_tokens_",
            suffix=".json",
        )
        tmp_path = Path(raw_tmp)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
            f.flush()
        os.replace(tmp_path, LOCAL_TOKENS_FILE)
    except Exception:
        if tmp_path is not None:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass


def load_local_broker_auth(provider: str, today: Optional[date] = None) -> Dict[str, str]:
    """Load locally persisted broker credentials and same-day access_token if dated today (IST)."""
    ref_day = today or datetime.now(IST).date()
    allowed = _TOKEN_KEEP_FIELDS.get(provider, ())
    if not allowed:
        return {}
    with _LOCAL_STORE_LOCK:
        store = _read_local_store_unlocked()
    entry = store.get(provider)
    if not isinstance(entry, dict):
        return {}

    out: Dict[str, str] = {}
    same_day = str(entry.get("token_date") or "").strip() == ref_day.isoformat()
    for field in allowed:
        val = str(entry.get(field) or "").strip()
        if field == "access_token":
            out[field] = val if (same_day and val) else ""
        elif val:
            out[field] = val
    return out


def save_local_broker_auth(
    provider: str,
    updates: Dict[str, str],
    clear_token: bool = False,
    today: Optional[date] = None,
) -> None:
    """Persist non-empty broker fields and/or today's access_token atomically to LOCAL_TOKENS_FILE."""
    ref_day = today or datetime.now(IST).date()
    allowed = set(_TOKEN_KEEP_FIELDS.get(provider, ()))
    if not allowed:
        return
    with _LOCAL_STORE_LOCK:
        store = _read_local_store_unlocked()
        entry = store.get(provider)
        cur = dict(entry) if isinstance(entry, dict) else {}

        if clear_token:
            cur.pop("access_token", None)
            cur.pop("token_date", None)

        changed = clear_token
        for field, raw_val in updates.items():
            if field not in allowed:
                continue
            val = str(raw_val or "").strip()
            if field == "access_token":
                if val:
                    if cur.get("access_token") != val or cur.get("token_date") != ref_day.isoformat():
                        cur["access_token"] = val
                        cur["token_date"] = ref_day.isoformat()
                        changed = True
            elif val:
                if cur.get(field) != val:
                    cur[field] = val
                    changed = True

        if changed:
            store[provider] = cur
            _write_local_store_unlocked(store)


def inspect_fivepaisa_jwt(access_token: str, now_ts: float | None = None) -> Dict[str, Any]:
    """
    Inspect 5paisa JWT claims to surface auto-detected ClientCode and IST expiry status in the UI.
    """
    token = (access_token or "").strip()
    claims = decode_jwt_claims(token)
    if not claims:
        return {
            "is_jwt": False,
            "client_code": "",
            "exp_ts": None,
            "is_expired": False,
            "exp_label": "",
        }

    client_code = extract_client_code_from_jwt(token)
    exp_raw = claims.get("exp")
    exp_ts: float | None = None
    is_expired = False
    exp_label = ""
    if exp_raw is not None:
        try:
            exp_val = float(exp_raw)
            if exp_val > 1e11:
                exp_val /= 1000.0
            exp_ts = exp_val
            ref_now = pytime.time() if now_ts is None else float(now_ts)
            is_expired = exp_val <= ref_now
            exp_label = datetime.fromtimestamp(exp_val, tz=IST).strftime("%d-%b %H:%M IST")
        except (TypeError, ValueError, OSError, OverflowError):
            exp_ts = None

    return {
        "is_jwt": True,
        "client_code": client_code,
        "exp_ts": exp_ts,
        "is_expired": is_expired,
        "exp_label": exp_label,
    }
