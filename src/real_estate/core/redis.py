"""
Centralized Redis Connection Pool Manager (Singleton).
Eliminates duplicate connection pools across repositories (Cache, Users, Rate Limiter).
"""

from typing import Optional
import redis.asyncio as aioredis
from real_estate.core.settings import settings
from real_estate.core.logger import logger

_redis_pool: Optional[aioredis.ConnectionPool] = None


def get_redis_pool() -> aioredis.ConnectionPool:
    """Returns the singleton Redis connection pool."""
    global _redis_pool
    if _redis_pool is None:
        _redis_pool = aioredis.ConnectionPool(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            password=settings.REDIS_PASSWORD or None,
            decode_responses=True,
            max_connections=30
        )
        logger.info(
            "redis_singleton_pool_initialized",
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            max_connections=30
        )
    return _redis_pool


def get_redis_client() -> aioredis.Redis:
    """Acquires an async Redis client from the shared connection pool."""
    return aioredis.Redis(connection_pool=get_redis_pool())


async def close_redis_pool() -> None:
    """Gracefully closes all connections in the Redis pool on shutdown."""
    global _redis_pool
    if _redis_pool is not None:
        await _redis_pool.disconnect()
        _redis_pool = None
        logger.info("redis_singleton_pool_closed")
