"""
Unified RAG Engine Endpoint (FastAPI Controller Layer).
Exposes the single unified End-to-End RAG Consultation & Discovery endpoint.
Protected by JWT Authentication and Sliding-Window Rate Limiting.
"""

from typing import Annotated
from fastapi import APIRouter, Depends, Header, Response
from real_estate.api.deps import get_rag_service, check_rate_limit, get_current_user
from real_estate.services.rag_service import RAGService
from real_estate.schemas.rag import RAGQueryRequest, RAGResponse
from real_estate.schemas.auth import TokenPayload

router = APIRouter(prefix="/rag", tags=["Agentic RAG Engine"])


@router.post(
    "",
    response_model=RAGResponse,
    dependencies=[Depends(check_rate_limit)],
    summary="End-to-End Conversational RAG Advisor & Discovery",
)
async def query_rag_advisor(
    request: RAGQueryRequest,
    response: Response,
    rag_service: Annotated[RAGService, Depends(get_rag_service)],
    current_user: Annotated[TokenPayload, Depends(get_current_user)],
    x_cache_bypass: Annotated[str | None, Header()] = None
):
    """
    Complete Unified RAG Consultation & Discovery Pipeline (Authenticated):
    1. Checks Two-Tier Cache (Exact Redis <2ms / Semantic Milvus >= 0.96 with active TTL & filter check).
    2. Extracts Egyptian Arabic query intent into structured filters (city, district, price, bedrooms, etc.).
    3. Retrieves top candidates via Dense INT8 Vector (Milvus) + Sparse Lexical (BM25) with RRF & Cross-Encoder re-ranking.
    4. Generates grounded natural Arabic advice (native llama.cpp / Gemini fallback).
    5. Saves complete result to Two-Tier Cache (6-Hour TTL).
    """
    if x_cache_bypass and x_cache_bypass.lower() == "true":
        request.bypass_cache = True

    result = await rag_service.execute_rag(request)

    if result.cached:
        response.headers["X-Cache"] = f"HIT-{result.cache_tier.upper() if result.cache_tier else 'CACHE'}"
    else:
        response.headers["X-Cache"] = "MISS"

    return result
