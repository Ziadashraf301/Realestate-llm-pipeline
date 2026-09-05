"""
System Health and Statistics Domain Schemas (Pydantic v2).
Enforces 100% strictly typed API contracts across all FastAPI health controller endpoints.
"""

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str = Field("healthy", description="System liveness status")
    environment: str = Field(..., description="Active runtime environment")
    app_name: str = Field(..., description="Application name")
    caching: str = Field(..., description="Two-Tier cache status and active TTL")
    vector_db: str = Field(..., description="Milvus vector database host and port configuration")
    llm_engine: str = Field(..., description="Probed primary local and fallback LLM engines status")


class SystemStatsResponse(BaseModel):
    success: bool = True
    total_properties: int = Field(..., ge=0, description="Total properties indexed in database")
    cache_ttl_hours: int = Field(..., ge=0, description="Two-Tier cache TTL in hours")
    semantic_similarity_threshold: float = Field(..., ge=0.0, le=1.0, description="Semantic vector similarity threshold")
    rate_limit_per_minute: int = Field(..., ge=1, description="Sliding-window request rate limit per minute")
