"""
Unit Tests for Two-Tier Caching Architecture (Milestone 6).
Validates Tier-1 Exact Match SHA-256 Hashing, Cache Invalidation, and Semantic Matching logic.
"""

import pytest
from unittest.mock import AsyncMock
from real_estate.services.cache_service import TwoTierCacheService
from real_estate.repositories.base import BaseCacheRepository, BaseVectorRepository


class MockCacheRepo(BaseCacheRepository):
    def __init__(self):
        self.store = {}

    async def get_exact(self, key: str) -> str | None:
        return self.store.get(key)

    async def set_exact(self, key: str, value: str, ttl_seconds: int) -> None:
        self.store[key] = value

    async def flush(self) -> None:
        self.store.clear()

    async def check_rate_limit(self, identifier: str, limit_per_minute: int) -> bool:
        return True

    async def is_url_scraped(self, url: str) -> bool:
        return False

    async def mark_url_scraped(self, url: str) -> None:
        pass


class MockVectorRepo(BaseVectorRepository):
    async def hybrid_search(self, query_vector, query_text, top_k=20, filters=None):
        return []

    async def search_semantic_cache(self, query_vector, threshold=0.96, filters=None):
        return None

    async def insert_semantic_cache(self, query_vector, query_text, response_json, expires_at, filters=None):
        pass

    async def upsert_property_vector(self, property_id, vector, payload):
        pass

    async def upsert_property_vectors_batch(self, items):
        pass

    async def delete_property_vector(self, property_id):
        pass


@pytest.mark.asyncio
async def test_exact_cache_hit_and_miss():
    cache_repo = MockCacheRepo()
    vector_repo = MockVectorRepo()
    service = TwoTierCacheService(cache_repo=cache_repo, vector_repo=vector_repo)

    query = "شقة للبيع في سموحة"

    # 1. First lookup should be a cache miss
    hit = await service.get(query)
    assert hit is None

    # 2. Store response in cache
    mock_response = {"message": "success", "results": []}
    await service.set(query=query, response_data=mock_response)

    # 3. Second lookup should be a Tier-1 cache hit
    hit = await service.get(query)
    assert hit is not None
    assert hit["source"] == "exact_cache"
    assert hit["similarity"] == 1.0
    assert hit["data"] == mock_response


@pytest.mark.asyncio
async def test_cache_invalidation():
    cache_repo = MockCacheRepo()
    vector_repo = MockVectorRepo()
    service = TwoTierCacheService(cache_repo=cache_repo, vector_repo=vector_repo)

    query = "شقة في المعادي"
    await service.set(query=query, response_data={"data": 123})

    # Verify present
    hit = await service.get(query)
    assert hit is not None

    # Invalidate
    await service.invalidate()

    # Verify cleared
    hit_after = await service.get(query)
    assert hit_after is None


def test_semantic_cache_filter_hashing():
    from real_estate.repositories.vector_repository import _hash_filters

    filters_a = {"city": "Alexandria", "min_bedrooms": 3, "max_price": 3000000}
    filters_b = {"city": "Alexandria", "min_bedrooms": 3, "max_price": 3000000}
    filters_c = {"city": "Alexandria", "min_bedrooms": 1, "max_price": 3000000}

    assert _hash_filters(filters_a) == _hash_filters(filters_b)
    assert _hash_filters(filters_a) != _hash_filters(filters_c)
    assert _hash_filters(None) == "none"
    assert _hash_filters({}) == "none"
