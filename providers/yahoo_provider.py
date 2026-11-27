"""
Yahoo Finance MarketDataProvider wrapping yahoo_client.
"""

from datetime import date, time
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import yahoo_client
from providers.base import ArrayBundle, TodaySummary


class YahooProvider:
    """Default zero-login market data provider backed by Yahoo Chart HTTP/2 API + yfinance fallback."""

    provider_id: str = "yahoo"
    display_name: str = "Yahoo Finance"
    cache_namespace: str = ""
    supports_coalesced_range: bool = True

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
        return yahoo_client._fetch_market_data(
            symbol_periods,
            interval=interval,
            max_workers=max_workers,
            start=start,
            end=end,
            chunk_size=chunk_size,
            symbol_ranges=symbol_ranges,
        )

    def fetch_today_summaries(
        self,
        symbols: List[str],
        today: date,
        cutoff: time,
        interval: str,
        max_workers: int = 8,
    ) -> Tuple[Optional[Dict[str, TodaySummary]], Dict[str, Any]]:
        return None, {"provider": self.provider_id, "mode": "rest"}

    def get_last_error(self) -> Optional[str]:
        return None
