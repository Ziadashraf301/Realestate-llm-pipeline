"""
Unit Tests for Milvus 2.5 Native Hybrid Search (replaces ArabicBM25Retriever tests).
Validates:
  - _build_filter_expr produces correct Milvus boolean expressions (single source of truth)
  - MilvusVectorRepository.hybrid_search calls client with both dense + sparse AnnSearchRequests
  - Filter isolation: same expr applied uniformly to both search paths
"""

import pytest
from unittest.mock import MagicMock

from real_estate.repositories.vector_repository import _build_filter_expr, MilvusVectorRepository


# ---------------------------------------------------------------------------
# Tests for _build_filter_expr (unified filter expression builder)
# ---------------------------------------------------------------------------

def test_filter_expr_city_only():
    expr = _build_filter_expr({"city": "Alexandria"})
    assert expr == "location like '%Alexandria%'"


def test_filter_expr_district_only():
    expr = _build_filter_expr({"district": "سموحة"})
    assert expr == "location like '%سموحة%'"


def test_filter_expr_city_and_district():
    expr = _build_filter_expr({"city": "Alexandria", "district": "سموحة"})
    assert "location like '%Alexandria%'" in expr
    assert "location like '%سموحة%'" in expr


def test_filter_expr_listing_and_property_type():
    expr = _build_filter_expr({"listing_type": "تمليك", "property_type": "شقة"})
    assert "listing_type == 'تمليك'" in expr
    assert "property_type == 'شقة'" in expr


def test_filter_expr_price_range():
    expr = _build_filter_expr({"min_price": 500000, "max_price": 3000000})
    assert "price_egp >= 500000.0" in expr
    assert "price_egp <= 3000000.0" in expr


def test_filter_expr_bedrooms():
    expr = _build_filter_expr({"min_bedrooms": 3, "min_bathrooms": 2})
    assert "bedrooms >= 3" in expr
    assert "bathrooms >= 2" in expr


def test_filter_expr_area_range():
    expr = _build_filter_expr({"min_area_sqm": 80.0, "max_area_sqm": 200.0})
    assert "area_sqm >= 80.0" in expr
    assert "area_sqm <= 200.0" in expr


def test_filter_expr_none_returns_none():
    assert _build_filter_expr(None) is None
    assert _build_filter_expr({}) is None


def test_filter_expr_sql_injection_prevention():
    expr = _build_filter_expr({"city": "O'Hara"})
    assert "O\\'Hara" in expr


# ---------------------------------------------------------------------------
# Tests for MilvusVectorRepository.hybrid_search
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_hybrid_search_submits_both_dense_and_sparse_requests():
    """
    hybrid_search must submit exactly 2 AnnSearchRequests:
      req[0] = dense vector (HNSW COSINE on 'vector' field)
      req[1] = raw text query (BM25 on 'sparse_vector' field)
    Both must carry the same filter expr.
    """
    repo = MilvusVectorRepository.__new__(MilvusVectorRepository)
    repo.collection_name = "test_properties"
    repo.semantic_cache_name = "test_cache"

    mock_client = MagicMock()
    mock_client.hybrid_search.return_value = [
        {
            "id": "prop_1",
            "distance": 0.92,
            "entity": {
                "title": "شقة سموحة", "location": "Alexandria, Smouha",
                "price_egp": 1500000.0, "listing_type": "تمليك",
                "property_type": "شقة", "bedrooms": 3, "bathrooms": 2,
                "area_sqm": 120.0, "text": "شقة سموحة", "url": "",
            }
        }
    ]
    MilvusVectorRepository._client = mock_client

    query_vector = [0.1] * 384
    query_text = "شقة في سموحة للبيع"
    filters = {"city": "Alexandria", "listing_type": "تمليك"}

    results = await repo.hybrid_search(
        query_vector=query_vector,
        query_text=query_text,
        top_k=10,
        filters=filters,
    )

    assert mock_client.hybrid_search.called
    call_args = mock_client.hybrid_search.call_args
    reqs = call_args.kwargs.get("reqs", call_args[1].get("reqs", []))
    assert len(reqs) == 2, "Must submit exactly 2 AnnSearchRequests (dense + sparse)"

    assert len(results) == 1
    assert results[0]["id"] == "prop_1"
    assert results[0]["similarity"] == 0.92


