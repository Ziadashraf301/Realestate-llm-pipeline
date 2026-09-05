"""Repository Layer (Data Access Abstractions)."""

from real_estate.repositories.base import (
    BaseCacheRepository,
    BaseVectorRepository,
    BasePropertyRepository,
    BaseUserRepository,
)
from real_estate.repositories.cache_repository import RedisCacheRepository
from real_estate.repositories.vector_repository import MilvusVectorRepository
from real_estate.repositories.property_repository import InMemoryPropertyRepository, RedisPropertyRepository
from real_estate.repositories.user_repository import InMemoryUserRepository, RedisUserRepository
from real_estate.repositories.warehouse_repository import ClickHouseWarehouseRepository

__all__ = [
    "BaseCacheRepository",
    "BaseVectorRepository",
    "BasePropertyRepository",
    "BaseUserRepository",
    "RedisCacheRepository",
    "MilvusVectorRepository",
    "InMemoryPropertyRepository",
    "RedisPropertyRepository",
    "InMemoryUserRepository",
    "RedisUserRepository",
    "ClickHouseWarehouseRepository",
]
