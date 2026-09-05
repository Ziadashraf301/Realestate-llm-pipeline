"""
Search and Recommendation Schemas (Pydantic v2).
"""

from typing import Any, Literal
from pydantic import BaseModel, Field
from real_estate.schemas.property import PropertyRead


class SearchQueryRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=500, description="Raw user query in Egyptian Arabic or English")
    n_results: int = Field(default=10, ge=1, le=50, description="Number of results to retrieve")
    location: str | None = Field(None, description="Optional override city")
    district: str | None = Field(None, description="Optional override district")
    listing_type: str | None = Field(None, description="Optional override listing type")
    property_type: str | None = Field(None, description="Optional override property type")
    min_price: float | None = Field(None, ge=0, description="Optional minimum price")
    max_price: float | None = Field(None, ge=0, description="Optional maximum price")
    min_bedrooms: int | None = Field(None, ge=0, description="Optional minimum bedrooms")
    min_bathrooms: int | None = Field(None, ge=0, description="Optional minimum bathrooms")
    min_area_sqm: float | None = Field(None, ge=0, description="Optional minimum area")
    max_area_sqm: float | None = Field(None, ge=0, description="Optional maximum area")
    bypass_cache: bool = Field(default=False, description="Explicit flag to bypass two-tier cache")


class SearchResultResponse(BaseModel):
    success: bool = True
    cached: bool = False
    cache_tier: Literal["exact_cache", "semantic_cache"] | None = None
    similarity_score: float | None = None
    latency_ms: float = Field(..., description="Total execution latency in milliseconds")
    count: int
    results: list[PropertyRead]


class GenerateSummaryRequest(BaseModel):
    query: str = Field(..., min_length=1)
    properties: list[dict[str, Any]] = Field(..., description="List of top property candidates")


class GenerateSummaryResponse(BaseModel):
    success: bool = True
    summary: str = Field(..., description="Markdown or HTML conversational Arabic recommendation")
