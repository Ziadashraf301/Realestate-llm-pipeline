"""
Property Relational/Metadata Repository (CRUD operations).
Persists property listings, metadata, and timeline indexes to Redis.
"""

import json
import time
import uuid
from typing import Any, Optional, cast
import redis.asyncio as aioredis

from real_estate.core.redis import get_redis_client
from real_estate.core.logger import logger
from real_estate.repositories.base import BasePropertyRepository
from real_estate.schemas.property import PropertyRead, PropertyCreate, PropertyUpdate


class RedisPropertyRepository(BasePropertyRepository):
    """
    Production Redis Property Metadata Repository.
    Persists property JSON records at key `property:{property_id}`
    and maintains chronological ordering in sorted set `properties:timeline`.
    """

    def __init__(self, client: Optional[aioredis.Redis] = None):
        self._custom_client = client

    def _get_client(self) -> aioredis.Redis:
        return self._custom_client or get_redis_client()

    async def get_by_id(self, property_id: str) -> PropertyRead | None:
        client = self._get_client()
        raw = await client.get(f"property:{property_id}")
        if not raw:
            return None
        return PropertyRead.model_validate_json(raw)

    async def list_properties(self, limit: int = 20, offset: int = 0) -> list[PropertyRead]:
        client = self._get_client()
        # Retrieve property IDs from sorted set by timeline (newest first)
        prop_ids = await client.zrevrange("properties:timeline", offset, offset + limit - 1)
        if not prop_ids:
            return []

        keys = [f"property:{pid.decode() if isinstance(pid, bytes) else pid}" for pid in prop_ids]
        raw_list = await client.mget(keys)
        results: list[PropertyRead] = []
        for raw in raw_list:
            if raw:
                try:
                    results.append(PropertyRead.model_validate_json(raw))
                except Exception as e:
                    logger.warning("corrupted_property_json_skipped", error=str(e))
        return results

    async def create(self, property_in: PropertyCreate) -> PropertyRead:
        client = self._get_client()
        new_id = f"prop_{uuid.uuid4().hex[:8]}"
        prop = PropertyRead(
            id=new_id,
            **property_in.model_dump()
        )
        now_ts = time.time()

        async with client.pipeline(transaction=True) as pipe:
            pipe.set(f"property:{new_id}", prop.model_dump_json())
            pipe.zadd("properties:timeline", {new_id: now_ts})
            await pipe.execute()

        logger.info("property_persisted_to_redis", property_id=new_id, location=prop.location)
        return prop

    async def update(self, property_id: str, property_in: PropertyUpdate) -> PropertyRead | None:
        client = self._get_client()
        existing = await self.get_by_id(property_id)
        if not existing:
            return None

        update_data = property_in.model_dump(exclude_unset=True)
        updated_dict = existing.model_dump()
        updated_dict.update(update_data)
        updated_prop = PropertyRead(**updated_dict)

        await client.set(f"property:{property_id}", updated_prop.model_dump_json())
        logger.info("property_updated_in_redis", property_id=property_id)
        return updated_prop

    async def delete(self, property_id: str) -> bool:
        client = self._get_client()
        async with client.pipeline(transaction=True) as pipe:
            pipe.delete(f"property:{property_id}")
            pipe.zrem("properties:timeline", property_id)
            res = await pipe.execute()

        deleted = bool(res[0] > 0 or res[1] > 0)
        if deleted:
            logger.info("property_deleted_from_redis", property_id=property_id)
        return deleted


# Backward-compatible alias
InMemoryPropertyRepository = RedisPropertyRepository

