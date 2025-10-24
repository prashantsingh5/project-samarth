"""
Data connectors for data.gov.in APIs with automatic retry, caching, and normalization.

This module provides high-level interfaces for fetching and processing agricultural
and climate data, abstracting away API details, caching, and data transformations.
"""

import httpx
import pandas as pd
from typing import Optional, Dict, Any
from tenacity import retry, stop_after_attempt, wait_exponential

from .cache import DataCache
from .normalizers import normalize_production, normalize_rainfall, aggregate_rainfall_to_state
from .mappings import SUBDIVISION_TO_STATE


class DataGovInConnector:
    """Connector for data.gov.in Resource API with built-in caching and normalization."""

    def __init__(self, api_key: str, cache: Optional[DataCache] = None):
        """
        Initialize connector.

        Args:
            api_key: data.gov.in API key
            cache: DataCache instance (creates new if None)
        """
        self.api_key = api_key
        self.cache = cache or DataCache()
        self.base_url = "https://api.data.gov.in/resource"

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _fetch_raw(self, resource_id: str, filters: Dict[str, Any], 
                   limit: int = 1000, use_cache: bool = True) -> list:
        """
        Fetch raw records from API with retry logic and caching.

        Args:
            resource_id: API resource ID
            filters: Query filters dict
            limit: Max records per page (default: 1000)
            use_cache: Whether to use cache (default: True)

        Returns:
            List of raw record dicts
        """
        # Check cache first
        if use_cache:
            cached = self.cache.get(resource_id, filters)
            if cached is not None:
                return cached

        # Fetch from API with pagination
        all_records = []
        offset = 0

        while True:
            params = {
                'api-key': self.api_key,
                'format': 'json',
                'limit': limit,
                'offset': offset,
                **filters
            }

            url = f"{self.base_url}/{resource_id}"
            resp = httpx.get(url, params=params, timeout=30)
            resp.raise_for_status()

            data = resp.json()
            records = data.get('records', [])
            all_records.extend(records)

            # Check if more pages exist
            if len(records) < limit:
                break
            offset += limit

        # Cache results
        if use_cache:
            self.cache.set(resource_id, filters, all_records)

        return all_records

    def fetch_production(
        self, 
        state: Optional[str] = None,
        district: Optional[str] = None,
        year: Optional[int] = None,
        crop: Optional[str] = None,
        season: Optional[str] = None,
        use_cache: bool = True
    ) -> pd.DataFrame:
        """
        Fetch and normalize production data.

        Args:
            state: Filter by state name (optional)
            district: Filter by district name (optional)
            year: Filter by crop year (optional)
            crop: Filter by crop name (optional)
            season: Filter by season (optional)
            use_cache: Whether to use cache (default: True)

        Returns:
            Normalized production DataFrame with canonical names
        """
        resource_id = "35be999b-0208-4354-b557-f6ca9a5355de"

        filters = {}
        if state:
            filters['filters[state_name]'] = state
        if district:
            filters['filters[district_name]'] = district
        if year:
            filters['filters[crop_year]'] = str(year)
        if crop:
            filters['filters[crop]'] = crop
        if season:
            filters['filters[season]'] = season

        raw_records = self._fetch_raw(resource_id, filters, use_cache=use_cache)
        return normalize_production(raw_records, apply_mappings=True)

    def fetch_rainfall(
        self,
        year_start: Optional[int] = None,
        year_end: Optional[int] = None,
        subdivision: Optional[str] = None,
        use_cache: bool = True
    ) -> pd.DataFrame:
        """
        Fetch and normalize rainfall data (subdivision-level).

        Args:
            year_start: Start year (optional, filters after fetch)
            year_end: End year (optional, filters after fetch)
            subdivision: Filter by subdivision (optional)
            use_cache: Whether to use cache (default: True)

        Returns:
            Normalized rainfall DataFrame (subdivision-level)

        Note:
            Rainfall API doesn't support year filters directly,
            so filtering happens after normalization.
        """
        resource_id = "8e0bd482-4aba-4d99-9cb9-ff124f6f1c2f"

        filters = {}
        if subdivision:
            filters['filters[subdivision]'] = subdivision

        raw_records = self._fetch_raw(resource_id, filters, use_cache=use_cache)
        df = normalize_rainfall(raw_records)

        # Apply year filters if specified
        if year_start:
            df = df[df['year'] >= year_start]
        if year_end:
            df = df[df['year'] <= year_end]

        return df

    def fetch_rainfall_by_state(
        self,
        state: str,
        year_start: Optional[int] = None,
        year_end: Optional[int] = None,
        use_cache: bool = True
    ) -> pd.DataFrame:
        """
        Fetch rainfall data aggregated to state level.

        Args:
            state: State name (canonical)
            year_start: Start year (optional)
            year_end: End year (optional)
            use_cache: Whether to use cache (default: True)

        Returns:
            State-level rainfall DataFrame
        """
        # Fetch subdivision-level data
        df_subdiv = self.fetch_rainfall(
            year_start=year_start,
            year_end=year_end,
            use_cache=use_cache
        )

        # Aggregate to state level
        df_state = aggregate_rainfall_to_state(df_subdiv)

        # Filter for requested state
        df_state = df_state[df_state['state_name'] == state]

        return df_state
