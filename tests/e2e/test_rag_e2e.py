"""
End-to-End RAG Consultation API Integration & E2E Tests.
Validates the unified authenticated POST /rag endpoint with Pydantic contracts.
"""

from unittest.mock import AsyncMock, MagicMock
import pytest
from real_estate.api.deps import get_rag_service
from real_estate.schemas.rag import RAGResponse
from real_estate.schemas.property import PropertyRead


@pytest.fixture
def mock_rag_service():
    mock = MagicMock()
    mock.execute_rag = AsyncMock(return_value=RAGResponse(
        success=True,
        query="عايز شقة 3 غرف في سموحة للبيع",
        recommendation="بناءً على طلبك في سموحة، نرشح لك الشقق التالية...",
        properties=[
            PropertyRead(
                id="prop_test_1",
                title="شقة فاخرة للبيع في سموحة",
                location="alexandria, smouha",
                listing_type="تمليك",
                property_type="Apartment",
                price_egp=2500000.0,
                bedrooms=3,
                bathrooms=2,
                area_sqm=160.0,
                description="شقة ممتازة بسعر لقطة"
            )
        ],
        cached=False,
        latency_ms=45.2
    ))
    return mock


def test_rag_advisor_authenticated_e2e(client, user_headers, mock_rag_service):
    """Verifies end-to-end authenticated POST /rag consultation pipeline."""
    from real_estate.main import app
    app.dependency_overrides[get_rag_service] = lambda: mock_rag_service

    try:
        response = client.post(
            "/api/v1/rag",
            json={"query": "شقة 3 غرف في سموحة", "n_results": 3},
            headers=user_headers
        )
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "recommendation" in data
        assert len(data["properties"]) == 1
        assert data["properties"][0]["id"] == "prop_test_1"
    finally:
        app.dependency_overrides.pop(get_rag_service, None)
