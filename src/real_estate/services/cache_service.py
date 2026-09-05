"""
Two-Tier Cache Service (SOLID: Single Responsibility).
Coordinates Tier-1 Exact Match (<2ms) and Tier-2 Semantic Vector Match (<15ms) with 6-Hour TTL.
"""

import asyncio
import hashlib
import json
import time
from typing import Any

from real_estate.core.settings import settings
from real_estate.core.logger import logger
from real_estate.repositories.base import BaseCacheRepository, BaseVectorRepository
from real_estate.retrieval.onnx_embedder import OnnxEmbeddingService


class TwoTierCacheService:
    """Enterprise Two-Tier Caching Service protecting LLM quotas with 6h TTL."""

    def __init__(
        self,
        cache_repo: BaseCacheRepository,
        vector_repo: BaseVectorRepository,
        embedder: OnnxEmbeddingService | None = None
    ):
        self.cache_repo = cache_repo
        self.vector_repo = vector_repo
        self.embedder = embedder or OnnxEmbeddingService()
        self.TTL = settings.CACHE_TTL_SECONDS  # 21,600s (6 Hours)
        self.THRESHOLD = settings.SEMANTIC_SIMILARITY_THRESHOLD  # 0.96

    def _hash_query(self, query: str, filters: dict[str, Any] | None = None) -> str:
        normalized = query.strip().lower()
        filter_str = json.dumps(filters or {}, sort_keys=True)
        combined = f"{normalized}:{filter_str}"
        return f"exact_cache:{hashlib.sha256(combined.encode()).hexdigest()}"

    async def get(
        self,
        query: str,
        filters: dict[str, Any] | None = None,
        bypass: bool = False
    ) -> dict[str, Any] | None:
        """Looks up cached recommendation with TTL and filter validation. Returns None on cache miss or bypass."""
        if bypass:
            logger.info("cache_bypassed_by_request", query=query)
            return None

        # 1. Tier 1: Exact Match Redis Lookup (< 2ms)
        exact_key = self._hash_query(query, filters=filters)
        exact_val = await self.cache_repo.get_exact(exact_key)
        if exact_val:
            logger.info("cache_hit_tier_1_exact", query=query)
            return {
                "source": "exact_cache",
                "similarity": 1.0,
                "data": json.loads(exact_val)
            }

        # 2. Tier 2: Semantic Vector Similarity Lookup with active TTL & Filter Match (< 15ms)
        try:
            query_vec_arr = await asyncio.to_thread(self.embedder.encode, query, is_query=True)
            query_vec = query_vec_arr.tolist()
            semantic_hit = await self.vector_repo.search_semantic_cache(
                query_vector=query_vec,
                filters=filters,
                threshold=self.THRESHOLD
            )
            if semantic_hit:
                logger.info("cache_hit_tier_2_semantic", query=query, similarity=semantic_hit["similarity"])
                raw_data = semantic_hit["data"]
                parsed = json.loads(raw_data) if isinstance(raw_data, str) else raw_data
                return {
                    "source": "semantic_cache",
                    "similarity": semantic_hit["similarity"],
                    "data": parsed
                }
        except Exception as e:
            logger.warning("semantic_cache_lookup_exception", error=str(e))

        return None

    async def get_exact(self, query: str) -> dict[str, Any] | None:
        """Fast Tier-1 Exact Match Redis Lookup (<2ms) without vector embedding or LLM extraction."""
        exact_key = self._hash_query(query, filters=None)
        exact_val = await self.cache_repo.get_exact(exact_key)
        if exact_val:
            try:
                data = json.loads(exact_val)
                logger.info("cache_hit_tier_1_exact_instant", query=query)
                return {
                    "source": "exact_cache",
                    "similarity": 1.0,
                    "data": data
                }
            except Exception as e:
                logger.debug("exact_cache_json_parse_error", error=str(e))
        return None

    async def set(
        self,
        query: str,
        response_data: Any,
        filters: dict[str, Any] | None = None
    ) -> None:
        """Stores recommendation with filter metadata in both exact and semantic cache with 6h TTL."""
        # Avoid caching 0-result empty responses permanently
        if isinstance(response_data, dict):
            props = response_data.get("properties")
            if isinstance(props, list) and len(props) == 0:
                logger.debug("skipping_cache_for_empty_properties", query=query)
                return
        elif isinstance(response_data, list) and len(response_data) == 0:
            logger.debug("skipping_cache_for_empty_list", query=query)
            return

        exact_key = self._hash_query(query, filters=filters)
        exact_raw_key = self._hash_query(query, filters=None)
        payload = json.dumps(response_data, ensure_ascii=False, default=str)

        # 1. Tier 1 Store (Redis SETEX)
        await self.cache_repo.set_exact(exact_key, payload, self.TTL)
        await self.cache_repo.set_exact(exact_raw_key, payload, self.TTL)

        # 2. Tier 2 Store (Milvus semantic collection with expires_at & filters)
        try:
            query_vec_arr = await asyncio.to_thread(self.embedder.encode, query, is_query=True)
            query_vec = query_vec_arr.tolist()
            expires_at = int(time.time()) + self.TTL
            await self.vector_repo.insert_semantic_cache(
                query_vector=query_vec,
                query_text=query,
                response_json=payload,
                expires_at=expires_at,
                filters=filters
            )
            logger.info("response_cached_in_two_tier_cache", query=query, ttl_seconds=self.TTL)
        except Exception as e:
            logger.warning("semantic_cache_store_failed", error=str(e))

    async def invalidate(self) -> None:
        """Flushes exact cache and invalidates semantic cache upon Property mutations."""
        logger.info("invalidating_cache_on_property_crud_mutation")
        await self.cache_repo.flush()
