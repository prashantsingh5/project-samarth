"""
Simple file-based cache for API responses.

This module provides a caching layer to avoid API rate limits and speed up
repeated queries during Q&A system execution.
"""

import json
import hashlib
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, Any


class DataCache:
    """File-based cache with TTL (Time-To-Live) support."""

    def __init__(self, cache_dir: str = '.cache', ttl_hours: int = 24):
        """
        Initialize cache.

        Args:
            cache_dir: Directory to store cache files (default: .cache)
            ttl_hours: Time-to-live in hours (default: 24 hours)
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.ttl = timedelta(hours=ttl_hours)

    def _generate_key(self, resource_id: str, filters: Dict[str, Any]) -> str:
        """
        Generate cache key from resource and filters.

        Args:
            resource_id: API resource ID
            filters: Query filters dict

        Returns:
            MD5 hash as cache key
        """
        # Sort filters for consistent hashing
        key_str = f"{resource_id}_{json.dumps(filters, sort_keys=True)}"
        return hashlib.md5(key_str.encode()).hexdigest()

    def get(self, resource_id: str, filters: Dict[str, Any]) -> Optional[list]:
        """
        Retrieve cached data if available and fresh.

        Args:
            resource_id: API resource ID
            filters: Query filters dict

        Returns:
            Cached records list or None if expired/not found
        """
        key = self._generate_key(resource_id, filters)
        cache_file = self.cache_dir / f"{key}.json"

        if not cache_file.exists():
            return None

        try:
            with open(cache_file) as f:
                cached = json.load(f)

            # Check if expired
            cached_time = datetime.fromisoformat(cached['timestamp'])
            if datetime.now() - cached_time > self.ttl:
                cache_file.unlink()  # Delete expired cache
                return None

            return cached['data']
        except Exception:
            return None

    def set(self, resource_id: str, filters: Dict[str, Any], data: list):
        """
        Store data in cache with current timestamp.

        Args:
            resource_id: API resource ID
            filters: Query filters dict
            data: Records list to cache
        """
        key = self._generate_key(resource_id, filters)
        cache_file = self.cache_dir / f"{key}.json"

        cached = {
            'timestamp': datetime.now().isoformat(),
            'resource_id': resource_id,
            'filters': filters,
            'data': data
        }

        with open(cache_file, 'w') as f:
            json.dump(cached, f, indent=2)

    def clear(self):
        """Clear all cached files."""
        for cache_file in self.cache_dir.glob('*.json'):
            cache_file.unlink()

    def stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.

        Returns:
            Dict with cache file count and total size
        """
        files = list(self.cache_dir.glob('*.json'))
        total_size = sum(f.stat().st_size for f in files)

        return {
            'file_count': len(files),
            'total_size_bytes': total_size,
            'total_size_mb': round(total_size / (1024 * 1024), 2)
        }
