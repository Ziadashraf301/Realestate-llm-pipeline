"""
User Repository for Authentication & Quota Management (Data Access Layer).
Persists user accounts, hashed credentials, and quotas directly to Redis.
Strictly decoupled: zero in-memory fallback dictionaries, zero embedded seed data.
"""

import json
import uuid
from typing import Any, Optional, cast
import redis.asyncio as aioredis

from real_estate.core.redis import get_redis_client
from real_estate.core.logger import logger
from real_estate.repositories.base import BaseUserRepository
from real_estate.schemas.auth import UserRead, UserRegister


class RedisUserRepository(BaseUserRepository):
    """
    Production Redis User Repository.
    Stores user account JSON at key `user:{user_id}` and maps normalized username
    to user ID at key `user_index:{username.lower()}`.
    """

    def __init__(self, client: Optional[aioredis.Redis] = None):
        self._custom_client = client

    def _get_client(self) -> aioredis.Redis:
        return self._custom_client or get_redis_client()

    async def get_by_username(self, username: str) -> dict[str, Any] | None:
        """Retrieves raw user record (including hashed password) by username."""
        client = self._get_client()
        norm_user = username.strip().lower()
        user_id = await client.get(f"user_index:{norm_user}")
        if not user_id:
            return None

        raw = await client.get(f"user:{user_id}")
        if not raw:
            return None

        return cast(dict[str, Any], json.loads(raw))

    async def get_by_id(self, user_id: str) -> UserRead | None:
        """Retrieves sanitized user profile by user ID."""
        client = self._get_client()
        raw = await client.get(f"user:{user_id}")
        if not raw:
            return None

        data = json.loads(raw)
        return UserRead(
            id=data["id"],
            username=data["username"],
            email=data["email"],
            role=data["role"],
            is_active=data.get("is_active", True),
            searches_remaining=data.get("searches_remaining", 50)
        )

    async def create(self, user_in: UserRegister, hashed_password: str) -> UserRead:
        """Persists a new user record atomically in Redis."""
        client = self._get_client()
        norm_user = user_in.username.strip().lower()

        # Verify uniqueness
        existing_id = await client.get(f"user_index:{norm_user}")
        if existing_id:
            raise ValueError(f"Username '{user_in.username}' already exists.")

        user_id = f"user_{uuid.uuid4().hex[:8]}"
        user_data = {
            "id": user_id,
            "username": user_in.username,
            "email": user_in.email,
            "hashed_password": hashed_password,
            "role": user_in.role,
            "is_active": True,
            "searches_remaining": 50
        }

        async with client.pipeline(transaction=True) as pipe:
            pipe.set(f"user:{user_id}", json.dumps(user_data))
            pipe.set(f"user_index:{norm_user}", user_id)
            await pipe.execute()

        logger.info("user_persisted_to_redis", user_id=user_id, username=user_in.username, role=user_in.role)

        return UserRead(
            id=user_id,
            username=user_in.username,
            email=user_in.email,
            role=user_in.role,
            is_active=True,
            searches_remaining=50
        )


# Backward-compatible alias
InMemoryUserRepository = RedisUserRepository
