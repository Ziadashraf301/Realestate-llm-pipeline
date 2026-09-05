"""
End-to-End RAG Schemas (Pydantic v2).
"""

from typing import Literal
from pydantic import BaseModel, Field
from real_estate.schemas.property import PropertyRead
from real_estate.schemas.intent import ExtractedQueryIntent


class RAGQueryRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=500, description="Raw query in colloquial Egyptian Arabic or English")
    n_results: int = Field(default=5, ge=1, le=20, description="Number of properties to retrieve as context")
    bypass_cache: bool = Field(default=False, description="Flag to force live retrieval and generation")


class RAGResponse(BaseModel):
    success: bool = True
    query: str
    intent: ExtractedQueryIntent | None = None
    recommendation: str = Field(..., description="Grounded natural Arabic recommendation from LLM")
    properties: list[PropertyRead]
    cached: bool = False
    cache_tier: Literal["exact_cache", "semantic_cache"] | None = None
    similarity_score: float | None = None
    latency_ms: float = Field(..., description="Total pipeline latency in milliseconds")
