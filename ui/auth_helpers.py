"""
Pure helpers for sidebar broker authentication: OAuth callback URL/param extraction and 5paisa JWT inspection.
"""

from __future__ import annotations

from datetime import datetime
import time as pytime
from typing import Any, Dict, Sequence
from urllib.parse import parse_qs, urlparse

from core.utils import IST
from providers.fivepaisa.stream import decode_jwt_claims, extract_client_code_from_jwt


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
