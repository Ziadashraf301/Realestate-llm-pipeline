"""
FastAPI v1 API Router Aggregator.
"""

from fastapi import APIRouter
from real_estate.api.v1.endpoints import health, auth, properties, rag

api_v1_router = APIRouter(prefix="/api/v1")

# Mount endpoints
api_v1_router.include_router(health.router)
api_v1_router.include_router(auth.router)
api_v1_router.include_router(properties.router)
api_v1_router.include_router(rag.router)
