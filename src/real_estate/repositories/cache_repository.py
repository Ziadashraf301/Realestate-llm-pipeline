"""
Pure Redis Cache & Rate Limiter Repository (Singleton Connection Pool).
Zero in-memory dictionary fallbacks. Strictly delegates to real Redis server.
"""

import time
import redis.asyncio as aioredis

from real_estate.core.settings import settings
from real_estate.core.logger import logger
from real_estate.repositories.base import BaseCacheRepository


from real_estate.core.redis import get_redis_client


class RedisCacheRepository(BaseCacheRepository):
    """
    Production Redis Repository.
    Manages Tier-1 Exact Match Caching (SHA-256 keys) and Token-Bucket Rate Limiting.
    Strictly uses Redis commands (GET, SETEX, FLUSHDB, ZADD, ZREMRANGEBYSCORE).
    """

    def _get_client(self) -> aioredis.Redis:
        """Acquires an active connection from the shared Redis singleton pool."""
        return get_redis_client()

    async def get_exact(self, key: str) -> str | None:
        """Retrieves exact match string from Redis key (<2ms)."""
        client = self._get_client()
        try:
            return await client.get(key)
        except Exception as e:
            logger.error("redis_get_command_failed", key=key, error=str(e))
            raise

    async def set_exact(self, key: str, value: str, ttl_seconds: int) -> None:
        """Stores key-value pair in Redis with TTL (default 21,600s = 6h)."""
        client = self._get_client()
        try:
            await client.setex(key, ttl_seconds, value)
        except Exception as e:
            logger.error("redis_setex_command_failed", key=key, error=str(e))
            raise

    async def flush(self) -> None:
        """Flushes Tier-1 exact search cache keys without destroying user records or scraper deduplication sets."""
        client = self._get_client()
        try:
            cursor = 0
            deleted_count = 0
            while True:
                cursor, keys = await client.scan(cursor=cursor, match="exact_cache:*", count=100)
                if keys:
                    await client.delete(*keys)
                    deleted_count += len(keys)
                if cursor == 0:
                    break
            logger.info("redis_search_cache_flushed_selectively", deleted_keys=deleted_count)
        except Exception as e:
            logger.error("redis_selective_flush_failed", error=str(e))
            raise

    async def check_rate_limit(self, identifier: str, limit_per_minute: int) -> bool:
        """
        Sliding-window rate limiter implemented purely in Redis via Sorted Sets (ZSET).
        Atomic pipeline: ZREMRANGEBYSCORE -> ZADD -> ZCARD -> EXPIRE.
        """
        now = time.time()
        client = self._get_client()
        key = f"rate_limit:{identifier}"

        try:
            async with client.pipeline(transaction=True) as pipe:
                # 1. Remove requests older than 60 seconds
                pipe.zremrangebyscore(key, 0, now - 60)
                # 2. Add current request timestamp
                pipe.zadd(key, {str(now): now})
                # 3. Count requests in the last 60 seconds
                pipe.zcard(key)
                # 4. Set TTL to auto-expire key after inactivity
                pipe.expire(key, 60)
                # Execute atomically
                _, _, count, _ = await pipe.execute()

            return count <= limit_per_minute
        except Exception as e:
            logger.error("redis_rate_limit_pipeline_failed", identifier=identifier, error=str(e))
            raise

    async def is_url_scraped(self, url: str) -> bool:
        """Checks if URL has been processed via Redis SET."""
        client = self._get_client()
        try:
            return bool(await client.sismember("scraped_property_urls", url))
        except Exception as e:
            logger.warning("redis_is_url_scraped_check_failed", url=url, error=str(e))
            return False

    async def mark_url_scraped(self, url: str) -> None:
        """Marks URL as processed in Redis SET."""
        client = self._get_client()
        try:
            await client.sadd("scraped_property_urls", url)
        except Exception as e:
            logger.warning("redis_mark_url_scraped_failed", url=url, error=str(e))
