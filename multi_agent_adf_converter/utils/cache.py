"""Cache manager for ADF scan results.

Avoids re-scanning Azure Data Factory on every run by caching the
raw catalog JSON to a local file. Subsequent runs load from cache
unless explicitly invalidated.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional


class CacheManager:
    """Manages local caching of ADF scan results."""

    def __init__(self, cache_dir: str = "cache", cache_ttl_hours: int = 24):
        """Initialize cache manager.

        Args:
            cache_dir: Directory to store cache files (relative or absolute).
            cache_ttl_hours: Cache validity period in hours.
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_ttl_seconds = cache_ttl_hours * 3600

    def _cache_key(self, factory_name: str, pipeline_names: Optional[list] = None) -> str:
        """Generate a deterministic cache key.

        Args:
            factory_name: ADF factory name.
            pipeline_names: Optional list of specific pipeline names.

        Returns:
            SHA256 hex digest used as filename.
        """
        raw = factory_name
        if pipeline_names:
            raw += "_" + "_".join(sorted(pipeline_names))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _cache_path(self, cache_key: str) -> Path:
        return self.cache_dir / f"{cache_key}.json"

    def get(self, factory_name: str, pipeline_names: Optional[list] = None) -> Optional[Dict[str, Any]]:
        """Retrieve cached ADF JSON if it exists and is still valid.

        Args:
            factory_name: ADF factory name.
            pipeline_names: Optional list of specific pipeline names.

        Returns:
            Cached JSON dict if valid, None otherwise.
        """
        key = self._cache_key(factory_name, pipeline_names)
        path = self._cache_path(key)

        if not path.exists():
            return None

        # Check TTL
        file_age = time.time() - path.stat().st_mtime
        if file_age > self.cache_ttl_seconds:
            os.remove(path)
            return None

        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def set(
        self,
        data: Dict[str, Any],
        factory_name: str,
        pipeline_names: Optional[list] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Cache ADF JSON to a local file.

        Args:
            data: The ADF catalog JSON to cache.
            factory_name: ADF factory name.
            pipeline_names: Optional list of specific pipeline names.
            metadata: Optional metadata dict (e.g., timestamp, stats).

        Returns:
            Path to the cache file as a string.
        """
        key = self._cache_key(factory_name, pipeline_names)
        path = self._cache_path(key)

        cache_entry = {
            "data": data,
            "metadata": metadata or {},
            "cached_at": time.time(),
            "factory_name": factory_name,
            "pipeline_names": pipeline_names,
        }

        with open(path, "w", encoding="utf-8") as f:
            json.dump(cache_entry, f, indent=2, default=str)

        return str(path)

    def invalidate(self, factory_name: str, pipeline_names: Optional[list] = None) -> bool:
        """Remove cached data for a given factory.

        Args:
            factory_name: ADF factory name.
            pipeline_names: Optional list of specific pipeline names.

        Returns:
            True if a cache file was removed, False otherwise.
        """
        key = self._cache_key(factory_name, pipeline_names)
        path = self._cache_path(key)
        if path.exists():
            os.remove(path)
            return True
        return False

    def clear_all(self) -> int:
        """Remove all cache files.

        Returns:
            Number of files removed.
        """
        count = 0
        for f in self.cache_dir.glob("*.json"):
            os.remove(f)
            count += 1
        return count
