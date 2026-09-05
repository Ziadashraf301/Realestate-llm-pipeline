"""
Initial RBAC Account Seeding Service.
Decoupled cleanly from the repository layer.
Invoked conditionally during database migration or application startup.
"""

import bcrypt
from typing import Optional, Literal, cast
from real_estate.core.logger import logger
from real_estate.repositories.base import BaseUserRepository
from real_estate.schemas.auth import UserRegister


def _hash_password(password: str) -> str:
    """Pre-hashes seed passwords with bcrypt."""
    salt = bcrypt.gensalt(rounds=12)
    return bcrypt.hashpw(password.encode("utf-8"), salt).decode("utf-8")


DEFAULT_SEED_USERS = [
    {
        "username": "admin",
        "email": "admin@realestate.ai",
        "password": "AdminPassword123!",
        "role": "admin"
    },
    {
        "username": "agent_alex",
        "email": "agent@realestate.ai",
        "password": "AgentPassword123!",
        "role": "agent"
    },
    {
        "username": "demo_user",
        "email": "demo@realestate.ai",
        "password": "UserPassword123!",
        "role": "user"
    }
]


async def seed_initial_users(user_repo: Optional[BaseUserRepository] = None) -> None:
    """Seeds default RBAC accounts (admin, agent, user) if not already created in Redis."""
    if user_repo is None:
        from real_estate.api.deps import get_user_repository
        user_repo = get_user_repository()
    for u in DEFAULT_SEED_USERS:
        existing = await user_repo.get_by_username(u["username"])
        if not existing:
            hashed = _hash_password(u["password"])
            user_in = UserRegister(
                username=u["username"],
                email=u["email"],
                password=u["password"],
                role=cast(Literal["admin", "agent", "user"], u["role"])
            )
            await user_repo.create(user_in, hashed)
            logger.info("seeded_default_rbac_user", username=u["username"], role=u["role"])
