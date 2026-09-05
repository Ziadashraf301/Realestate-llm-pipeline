"""
FastAPI Dependency Injection Providers (SOLID: Dependency Inversion).
Centralizes singletons and creates service instances for request lifecycles.
"""

from functools import lru_cache
from typing import Annotated
from fastapi import Depends, HTTPException, Security, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from real_estate.core.settings import settings
from real_estate.repositories.base import (
    BaseCacheRepository,
    BaseVectorRepository,
    BasePropertyRepository,
    BaseUserRepository,
)
from real_estate.repositories.cache_repository import RedisCacheRepository
from real_estate.repositories.vector_repository import MilvusVectorRepository
from real_estate.repositories.property_repository import RedisPropertyRepository
from real_estate.repositories.user_repository import RedisUserRepository
from real_estate.repositories.warehouse_repository import ClickHouseWarehouseRepository

from real_estate.services.cache_service import TwoTierCacheService
from real_estate.services.intent_service import IntentService
from real_estate.services.auth_service import AuthService
from real_estate.services.property_service import PropertyService
from real_estate.services.rag_service import RAGService
from real_estate.schemas.auth import TokenPayload

security_bearer = HTTPBearer(auto_error=False)

# --- Singleton Repositories ---

@lru_cache
def get_cache_repository() -> BaseCacheRepository:
    return RedisCacheRepository()

@lru_cache
def get_vector_repository() -> BaseVectorRepository:
    return MilvusVectorRepository()

@lru_cache
def get_property_repository() -> BasePropertyRepository:
    return RedisPropertyRepository()

@lru_cache
def get_user_repository() -> BaseUserRepository:
    return RedisUserRepository()

@lru_cache
def get_warehouse_repository() -> ClickHouseWarehouseRepository:
    return ClickHouseWarehouseRepository()

# --- Services ---

def get_cache_service(
    cache_repo: Annotated[BaseCacheRepository, Depends(get_cache_repository)],
    vector_repo: Annotated[BaseVectorRepository, Depends(get_vector_repository)],
) -> TwoTierCacheService:
    return TwoTierCacheService(cache_repo=cache_repo, vector_repo=vector_repo)

@lru_cache
def get_intent_service() -> IntentService:
    return IntentService()

def get_auth_service(
    user_repo: Annotated[BaseUserRepository, Depends(get_user_repository)],
) -> AuthService:
    return AuthService(user_repo=user_repo)

def get_property_service(
    property_repo: Annotated[BasePropertyRepository, Depends(get_property_repository)],
    vector_repo: Annotated[BaseVectorRepository, Depends(get_vector_repository)],
    cache_service: Annotated[TwoTierCacheService, Depends(get_cache_service)],
    warehouse_repo: Annotated[ClickHouseWarehouseRepository, Depends(get_warehouse_repository)],
) -> PropertyService:
    return PropertyService(
        property_repo=property_repo,
        vector_repo=vector_repo,
        cache_service=cache_service,
        warehouse_repo=warehouse_repo,
    )

def get_rag_service(
    cache_service: Annotated[TwoTierCacheService, Depends(get_cache_service)],
    vector_repo: Annotated[BaseVectorRepository, Depends(get_vector_repository)],
    intent_service: Annotated[IntentService, Depends(get_intent_service)],
) -> RAGService:
    """RAGService with Milvus 2.5 native hybrid search — no BM25 retriever injection needed."""
    return RAGService(
        cache_service=cache_service,
        vector_repo=vector_repo,
        intent_service=intent_service,
    )




# --- Auth & Rate Limiting Dependencies ---

async def get_current_user(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Security(security_bearer)],
    auth_service: Annotated[AuthService, Depends(get_auth_service)],
) -> TokenPayload:
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return auth_service.decode_token(credentials.credentials)

async def check_rate_limit(
    cache_repo: Annotated[BaseCacheRepository, Depends(get_cache_repository)],
    credentials: Annotated[HTTPAuthorizationCredentials | None, Security(security_bearer)],
) -> None:
    identifier = credentials.credentials[:16] if credentials else "anonymous_client"
    allowed = await cache_repo.check_rate_limit(
        identifier=identifier,
        limit_per_minute=settings.RATE_LIMIT_REQUESTS_PER_MINUTE,
    )
    if not allowed:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Rate limit exceeded. Maximum {settings.RATE_LIMIT_REQUESTS_PER_MINUTE} requests/min.",
        )


def require_roles(*allowed_roles: str):
    """
    Role-Based Access Control (RBAC) Dependency Factory.
    Enforces that the authenticated user possesses one of the allowed roles.
    Example: Depends(require_roles("agent", "admin"))
    """
    async def _role_checker(
        current_user: Annotated[TokenPayload, Depends(get_current_user)]
    ) -> TokenPayload:
        if current_user.role not in allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Access denied: Required role in {list(allowed_roles)}. Your current role is '{current_user.role}'."
            )
        return current_user

    return _role_checker

