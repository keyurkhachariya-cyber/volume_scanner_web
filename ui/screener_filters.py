"""
Pure filtering, sorting, row styling, and auto-refresh scheduling logic for the screener UI.
"""

from datetime import datetime, time
from typing import Dict, Tuple

import pandas as pd

from core.oi import (
    SIGNAL_BEARISH,
    SIGNAL_BULLISH,
    classify_oi_buildup,
    classify_trade_signal,
)

MARKET_CLOSE_TIME = time(15, 30)

_SIGNAL_RANK = {SIGNAL_BULLISH: 0, SIGNAL_BEARISH: 1}
_SIGNAL_ROW_CSS = {
    SIGNAL_BULLISH: ("background-color: rgba(16, 185, 129, 0.16);", "color: #10b981; font-weight: 600;"),
    SIGNAL_BEARISH: ("background-color: rgba(239, 68, 68, 0.16);", "color: #ef4444; font-weight: 600;"),
}
_SIGNAL_ACCENT_COLS = {"Signal", "Pct_Change", "From_Open_Pct", "VWAP_Dist_Pct", "OI_Chg_Pct", "OI_Buildup"}


def should_auto_scan(
    now: datetime,
    last_scan_dt: datetime | None,
    refresh_seconds: int,
    market_open: bool,
    holidays_set: set,
    skip_auto_scan: bool,
    post_close_attempts: int = 0,
    max_post_close_attempts: int = 2,
    last_scan_started_dt: datetime | None = None,
) -> bool:
    """
    Decide whether a scheduled auto-refresh scan should execute:
    - Never run during pure UI/filter interactions.
    - Poll every refresh_seconds while NSE is open.
    - Allow a bounded post-close snapshot (up to max_post_close_attempts on empty data) after 15:30 IST.
    - Stay idle pre-market, on weekends, and on holidays.
    """
    if skip_auto_scan:
        return False

    if market_open:
        cadence_ref = last_scan_started_dt or last_scan_dt
        if cadence_ref is None or cadence_ref.date() != now.date():
            return True
        elapsed = (now - cadence_ref).total_seconds()
        return elapsed >= max(refresh_seconds - 1, 1)

    is_trading_day = now.weekday() < 5 and now.date() not in holidays_set
    if is_trading_day and now.time() > MARKET_CLOSE_TIME:
        if post_close_attempts >= max_post_close_attempts:
            return False
        market_close_dt = now.replace(hour=15, minute=30, second=0, microsecond=0)
        if last_scan_dt is None or last_scan_dt.date() < now.date() or last_scan_dt < market_close_dt:
            return True

    return False


def style_scanner_df(display_df: pd.DataFrame, signals: pd.Series):
    """Apply translucent green/red row highlights and text accents for Bullish/Bearish rows."""
    sig_map = dict(zip(display_df.index, signals))
    cols = list(display_df.columns)

    def _row_styles(row: pd.Series) -> list[str]:
        styles = _SIGNAL_ROW_CSS.get(sig_map.get(row.name))
        if styles is None:
            return [""] * len(cols)
        bg_css, accent_css = styles
        return [bg_css + (accent_css if col in _SIGNAL_ACCENT_COLS else "") for col in cols]

    return display_df.style.apply(_row_styles, axis=1)


def _backfill_derived_columns(raw_df: pd.DataFrame, include_oi: bool) -> pd.DataFrame:
    """Lazily compute derived columns when viewing older cached DataFrames."""
    working_df = raw_df

    def _ensure_copy() -> pd.DataFrame:
        return working_df.copy() if working_df is raw_df else working_df

    if "Gap_Pct" not in working_df.columns and {"Day_Open", "Prev_Close"}.issubset(working_df.columns):
        working_df = _ensure_copy()
        valid_prev = working_df["Prev_Close"].notna() & (working_df["Prev_Close"] > 0)
        working_df["Gap_Pct"] = float("nan")
        working_df.loc[valid_prev, "Gap_Pct"] = (
            (working_df.loc[valid_prev, "Day_Open"] - working_df.loc[valid_prev, "Prev_Close"])
            / working_df.loc[valid_prev, "Prev_Close"]
        ) * 100

    if "From_Open_Pct" not in working_df.columns and {"LTP", "Day_Open"}.issubset(working_df.columns):
        working_df = _ensure_copy()
        valid_open = working_df["Day_Open"].notna() & (working_df["Day_Open"] > 0) & working_df["LTP"].notna()
        working_df["From_Open_Pct"] = float("nan")
        working_df.loc[valid_open, "From_Open_Pct"] = (
            (working_df.loc[valid_open, "LTP"] - working_df.loc[valid_open, "Day_Open"])
            / working_df.loc[valid_open, "Day_Open"]
        ) * 100

    if "Range_Pos_Pct" not in working_df.columns and {"LTP", "Day_High", "Day_Low"}.issubset(working_df.columns):
        working_df = _ensure_copy()
        span = working_df["Day_High"] - working_df["Day_Low"]
        valid_rng = working_df["LTP"].notna() & span.notna() & (span > 0)
        working_df["Range_Pos_Pct"] = float("nan")
        working_df.loc[valid_rng, "Range_Pos_Pct"] = (
            ((working_df.loc[valid_rng, "LTP"] - working_df.loc[valid_rng, "Day_Low"]) / span.loc[valid_rng]) * 100
        ).clip(0.0, 100.0)

    if include_oi and "OI_Buildup" not in working_df.columns and {"Pct_Change", "OI_Chg"}.issubset(working_df.columns):
        working_df = _ensure_copy()
        working_df["OI_Buildup"] = [
            classify_oi_buildup(pct, oi_c)
            for pct, oi_c in zip(working_df["Pct_Change"], working_df["OI_Chg"])
        ]

    if include_oi and "Signal" not in working_df.columns and {"Ratio", "OI_Chg_Pct", "Pct_Change"}.issubset(working_df.columns):
        working_df = _ensure_copy()
        working_df["Signal"] = [
            classify_trade_signal(r, o, p)
            for r, o, p in zip(working_df["Ratio"], working_df["OI_Chg_Pct"], working_df["Pct_Change"])
        ]

    return working_df


def _apply_gap_filter(
    df: pd.DataFrame,
    mask: pd.Series,
    base_mask: pd.Series,
    exclude_gap_up: bool,
    exclude_gap_down: bool,
    gap_threshold: float,
) -> Tuple[pd.Series, bool, int]:
    if not (exclude_gap_up or exclude_gap_down):
        return mask, False, 0
    if "Gap_Pct" not in df.columns:
        return mask, True, 0
    gap_vals = pd.to_numeric(df["Gap_Pct"], errors="coerce")
    if exclude_gap_up:
        mask &= gap_vals.isna() | (gap_vals <= gap_threshold)
    if exclude_gap_down:
        mask &= gap_vals.isna() | (gap_vals >= -gap_threshold)
    return mask, False, int(base_mask.sum() - mask.sum())


def _apply_oi_filter(
    df: pd.DataFrame,
    mask: pd.Series,
    include_oi: bool,
    min_oi_chg_pct: float | None,
    oi_buildup_filter: tuple[str, ...],
) -> Tuple[pd.Series, bool, int]:
    if not (include_oi and (min_oi_chg_pct is not None or oi_buildup_filter)):
        return mask, False, 0

    pre_oi_count = int(mask.sum())
    oi_missing = False
    if min_oi_chg_pct is not None:
        if "OI_Chg_Pct" not in df.columns:
            oi_missing = True
        else:
            oi_pct_vals = pd.to_numeric(df["OI_Chg_Pct"], errors="coerce")
            mask &= oi_pct_vals.notna() & (oi_pct_vals >= float(min_oi_chg_pct))

    if oi_buildup_filter:
        if "OI_Buildup" not in df.columns:
            oi_missing = True
        else:
            mask &= df["OI_Buildup"].isin(oi_buildup_filter)

    return mask, oi_missing, (0 if oi_missing else pre_oi_count - int(mask.sum()))


def _apply_momentum_filter(
    df: pd.DataFrame,
    mask: pd.Series,
    momentum_filter_state: tuple[bool, bool, int, float] | None,
) -> Tuple[pd.Series, bool, int]:
    if momentum_filter_state is None:
        return mask, False, 0

    req_vwap, req_open, edge_pct, min_burst = momentum_filter_state
    pre_mom_count = int(mask.sum())
    momentum_missing = False
    pct_vals = pd.to_numeric(df["Pct_Change"], errors="coerce")
    fo_vals_all = (
        pd.to_numeric(df["From_Open_Pct"], errors="coerce")
        if "From_Open_Pct" in df.columns
        else pd.Series(float("nan"), index=df.index)
    )
    # Use daily % Chg direction; for flat 0.00% vs prev close, fall back to intraday candle direction.
    is_up = (pct_vals > 0) | ((pct_vals == 0) & (fo_vals_all > 0))
    is_down = (pct_vals < 0) | ((pct_vals == 0) & (fo_vals_all < 0))

    if req_vwap:
        if "VWAP_Dist_Pct" not in df.columns:
            momentum_missing = True
        else:
            vw_vals = pd.to_numeric(df["VWAP_Dist_Pct"], errors="coerce")
            mask &= (is_up & vw_vals.notna() & (vw_vals > 0)) | (is_down & vw_vals.notna() & (vw_vals < 0))

    if req_open:
        if "From_Open_Pct" not in df.columns:
            momentum_missing = True
        else:
            fo_vals = pd.to_numeric(df["From_Open_Pct"], errors="coerce")
            mask &= (is_up & fo_vals.notna() & (fo_vals > 0)) | (is_down & fo_vals.notna() & (fo_vals < 0))

    if edge_pct <= 50:
        if "Range_Pos_Pct" not in df.columns:
            momentum_missing = True
        else:
            rp_vals = pd.to_numeric(df["Range_Pos_Pct"], errors="coerce")
            mask &= rp_vals.notna() & (
                (is_up & (rp_vals >= (100.0 - edge_pct))) | (is_down & (rp_vals <= float(edge_pct)))
            )

    if min_burst > 0:
        if "Vol_Burst_15m" not in df.columns:
            momentum_missing = True
        else:
            vb_vals = pd.to_numeric(df["Vol_Burst_15m"], errors="coerce")
            if vb_vals.notna().any():
                mask &= vb_vals.notna() & (vb_vals >= float(min_burst))

    return mask, momentum_missing, (0 if momentum_missing else pre_mom_count - int(mask.sum()))


def _sort_results(
    df: pd.DataFrame,
    sort_by: str,
    sort_options: Dict[str, Tuple[str, bool, bool]],
) -> pd.DataFrame:
    sort_col, sort_asc, sort_abs = sort_options.get(sort_by, ("Ratio", False, False))
    if sort_col not in df.columns or df.empty:
        return df
    if sort_col == "Signal":
        return (
            df.assign(_sig_rank=df["Signal"].map(_SIGNAL_RANK).fillna(2))
            .sort_values(["_sig_rank", "Ratio"], ascending=[True, False], na_position="last")
            .drop(columns=["_sig_rank"])
        )
    return df.sort_values(
        sort_col,
        ascending=sort_asc,
        key=(lambda s: pd.to_numeric(s, errors="coerce").abs())
        if sort_abs
        else (lambda s: pd.to_numeric(s, errors="coerce")),
        na_position="last",
    )


def filter_and_sort_results(
    raw_df: pd.DataFrame,
    min_ratio: float,
    min_volume: int,
    exclude_gap_up: bool,
    exclude_gap_down: bool,
    gap_threshold: float,
    include_oi: bool,
    min_oi_chg_pct: float | None,
    oi_buildup_filter: tuple[str, ...],
    sort_by: str,
    sort_options: Dict[str, Tuple[str, bool, bool]],
    momentum_filter_state: tuple[bool, bool, int, float] | None = None,
) -> tuple[pd.DataFrame, bool, int, bool, int, bool, int]:
    """Apply volume, gap, OI threshold/buildup, and momentum quality filters plus sorting in memory."""
    if raw_df.empty:
        return raw_df, False, 0, False, 0, False, 0

    working_df = _backfill_derived_columns(raw_df, include_oi)
    base_mask = (working_df["Ratio"] >= min_ratio) & (working_df["Vol_Today"] >= int(min_volume))
    mask = base_mask.copy()

    mask, gap_missing, gap_excluded_count = _apply_gap_filter(
        working_df, mask, base_mask, exclude_gap_up, exclude_gap_down, gap_threshold
    )
    mask, oi_missing, oi_excluded_count = _apply_oi_filter(
        working_df, mask, include_oi, min_oi_chg_pct, oi_buildup_filter
    )
    mask, momentum_missing, momentum_excluded_count = _apply_momentum_filter(
        working_df, mask, momentum_filter_state
    )

    df = _sort_results(working_df[mask].copy(), sort_by, sort_options)
    return (
        df.reset_index(drop=True),
        gap_missing,
        gap_excluded_count,
        oi_missing,
        oi_excluded_count,
        momentum_missing,
        momentum_excluded_count,
    )
