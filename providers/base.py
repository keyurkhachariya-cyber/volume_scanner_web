"""
Pluggable market data provider protocol and registry.
"""

from datetime import date, time
from typing import Any, Dict, Iterable, List, Optional, Protocol, Set, Tuple

import numpy as np

ArrayBundle = Dict[str, Optional[np.ndarray]]
TodaySummary = Tuple[
    float,
    Optional[float],
    Optional[float],
    Optional[float],
    Optional[float],
    Optional[time],
    Optional[float],
    Optional[float],
]


class MarketDataProvider(Protocol):
    """Protocol implemented by market data backends (Yahoo Finance, Upstox, etc.)."""

    provider_id: str
    display_name: str
    cache_namespace: str
    supports_coalesced_range: bool

    def fetch_bars(
        self,
        symbol_periods: Iterable[Tuple[str, Optional[str]]],
        interval: str,
        max_workers: int = 8,
        start: Optional[str] = None,
        end: Optional[str] = None,
        chunk_size: Optional[int] = None,
        symbol_ranges: Optional[Dict[str, Tuple[Optional[str], Optional[str]]]] = None,
    ) -> Tuple[Dict[str, ArrayBundle], Set[str]]:
        """Fetch OHLCV bar arrays keyed by symbol (.NS format) plus transiently failed symbols."""
        ...

    def fetch_today_summaries(
        self,
        symbols: List[str],
        today: date,
        cutoff: time,
        interval: str,
        max_workers: int = 8,
    ) -> Tuple[Optional[Dict[str, TodaySummary]], Dict[str, Any]]:
        """
        Optionally return live pre-summarized TodaySummary tuples (e.g. from a WebSocket feed)
        instead of re-fetching full intraday bars, plus diagnostic metadata.
        Returning None falls back to bar-based fetching via fetch_bars().
        """
        ...

    def get_last_error(self) -> Optional[str]:
        """Return latest human-readable provider error notice, if any."""
        ...
