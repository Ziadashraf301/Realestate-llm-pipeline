"""
Health & System Status Endpoints (FastAPI Controller Layer).
Provides real-time liveness, readiness, and storage statistics.
"""

from typing import Annotated
from fastapi import APIRouter, Depends, Request
from real_estate.core.settings import settings
from real_estate.api.deps import get_warehouse_repository, get_property_repository
from real_estate.repositories.warehouse_repository import ClickHouseWarehouseRepository
from real_estate.repositories.base import BasePropertyRepository
from real_estate.schemas.health import HealthResponse, SystemStatsResponse

router = APIRouter()


@router.get("/health", response_model=HealthResponse, tags=["System"])
async def health_check(request: Request):
    """System health check showing primary local llama.cpp and optional cloud fallback status."""
    probed_llm = getattr(request.app.state, "llm_engine_status", None)
    return {
        "status": "healthy",
        "environment": settings.ENVIRONMENT,
        "app_name": settings.APP_NAME,
        "caching": f"Two-Tier Active (TTL {settings.CACHE_TTL_SECONDS // 3600} Hours)",
        "vector_db": f"Milvus ({settings.MILVUS_HOST}:{settings.MILVUS_PORT})",
        "llm_engine": probed_llm or (
            "Native llama.cpp (Local CPU/GPU port 8080) [Primary] with Google Gemini 2.0 Flash [Fallback]"
            if settings.GOOGLE_API_KEY
            else "Native llama.cpp (Local CPU/GPU port 8080) [Primary Only]"
        ),
    }


@router.get("/stats", response_model=SystemStatsResponse, tags=["System"])
async def system_stats(
    warehouse: Annotated[ClickHouseWarehouseRepository, Depends(get_warehouse_repository)],
    property_repo: Annotated[BasePropertyRepository, Depends(get_property_repository)],
):
    """Returns dynamic system statistics directly from persistent data stores."""
    count = await warehouse.get_total_count()
    if count == 0:
        props = await property_repo.list_properties(limit=1000)
        count = len(props)

    return {
        "success": True,
        "total_properties": count,
        "cache_ttl_hours": settings.CACHE_TTL_SECONDS // 3600,
        "semantic_similarity_threshold": settings.SEMANTIC_SIMILARITY_THRESHOLD,
        "rate_limit_per_minute": settings.RATE_LIMIT_REQUESTS_PER_MINUTE,
    }
