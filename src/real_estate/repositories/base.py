"""
Abstract Base Repositories (SOLID: Interface Segregation & Dependency Inversion).
"""

from abc import ABC, abstractmethod
from typing import Any
from real_estate.schemas.property import PropertyRead, PropertyCreate, PropertyUpdate
from real_estate.schemas.auth import UserRead, UserRegister


class BaseCacheRepository(ABC):
    """Interface for Tier-1 exact key-value caching and rate limiting."""

    @abstractmethod
    async def get_exact(self, key: str) -> str | None:
        pass

    @abstractmethod
    async def set_exact(self, key: str, value: str, ttl_seconds: int) -> None:
        pass

    @abstractmethod
    async def flush(self) -> None:
        pass

    @abstractmethod
    async def check_rate_limit(self, identifier: str, limit_per_minute: int) -> bool:
        pass

    @abstractmethod
    async def is_url_scraped(self, url: str) -> bool:
        pass

    @abstractmethod
    async def mark_url_scraped(self, url: str) -> None:
        pass


class BaseVectorRepository(ABC):
    """Interface for Milvus vector database operations (Milvus 2.5 native hybrid search)."""

    @abstractmethod
    async def hybrid_search(
        self,
        query_vector: list[float],
        query_text: str,
        top_k: int = 20,
        filters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Native hybrid search: dense HNSW + BM25 sparse, fused by RRFRanker."""
        pass


    @abstractmethod
    async def search_semantic_cache(
        self,
        query_vector: list[float],
        filters: dict[str, Any] | None = None,
        threshold: float = 0.96,
    ) -> dict[str, Any] | None:
        pass

    @abstractmethod
    async def insert_semantic_cache(
        self,
        query_vector: list[float],
        query_text: str,
        response_json: str,
        expires_at: int,
        filters: dict[str, Any] | None = None,
    ) -> None:
        pass

    @abstractmethod
    async def upsert_property_vector(self, property_id: str, vector: list[float], payload: dict[str, Any]) -> None:
        pass

    @abstractmethod
    async def upsert_property_vectors_batch(self, items: list[dict[str, Any]]) -> None:
        """Batch upserts property vectors with payloads into Milvus in a single bulk operation."""
        pass

    @abstractmethod
    async def delete_property_vector(self, property_id: str) -> None:
        pass


class BasePropertyRepository(ABC):
    """Interface for relational/tabular property storage (BigQuery / SQLite)."""

    @abstractmethod
    async def get_by_id(self, property_id: str) -> PropertyRead | None:
        pass

    @abstractmethod
    async def list_properties(self, limit: int = 20, offset: int = 0) -> list[PropertyRead]:
        pass

    @abstractmethod
    async def create(self, property_in: PropertyCreate) -> PropertyRead:
        pass

    @abstractmethod
    async def update(self, property_id: str, property_in: PropertyUpdate) -> PropertyRead | None:
        pass

    @abstractmethod
    async def delete(self, property_id: str) -> bool:
        pass


class BaseUserRepository(ABC):
    """Interface for user accounts and auth persistence."""

    @abstractmethod
    async def get_by_username(self, username: str) -> dict[str, Any] | None:
        pass

    @abstractmethod
    async def get_by_id(self, user_id: str) -> UserRead | None:
        pass

    @abstractmethod
    async def create(self, user_in: UserRegister, hashed_password: str) -> UserRead:
        pass



