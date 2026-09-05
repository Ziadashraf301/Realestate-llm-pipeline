"""
Unit and Integration Tests for PropertyService and RBAC Enforcement.
Validates Clean Architecture: Controller -> Service -> Repository.
"""

import pytest
import uuid
from typing import Optional
from real_estate.repositories.base import BasePropertyRepository
from real_estate.schemas.property import PropertyCreate, PropertyUpdate, PropertyRead
from real_estate.services.property_service import PropertyService
from tests.unit.test_cache import MockCacheRepo, MockVectorRepo
from real_estate.services.cache_service import TwoTierCacheService


class MockPropertyRepo(BasePropertyRepository):
    def __init__(self):
        self._store: dict[str, PropertyRead] = {}

    async def get_by_id(self, property_id: str) -> Optional[PropertyRead]:
        return self._store.get(property_id)

    async def list_properties(self, limit: int = 20, offset: int = 0) -> list[PropertyRead]:
        items = list(self._store.values())
        return items[offset: offset + limit]

    async def create(self, property_in: PropertyCreate) -> PropertyRead:
        prop_id = f"prop_{uuid.uuid4().hex[:8]}"
        prop = PropertyRead(id=prop_id, **property_in.model_dump())
        self._store[prop_id] = prop
        return prop

    async def update(self, property_id: str, property_in: PropertyUpdate) -> Optional[PropertyRead]:
        existing = self._store.get(property_id)
        if not existing:
            return None
        updated_data = existing.model_dump()
        updated_data.update(property_in.model_dump(exclude_unset=True))
        updated = PropertyRead(**updated_data)
        self._store[property_id] = updated
        return updated

    async def delete(self, property_id: str) -> bool:
        if property_id in self._store:
            del self._store[property_id]
            return True
        return False


@pytest.mark.asyncio
async def test_property_service_crud_lifecycle():
    prop_repo = MockPropertyRepo()
    cache_repo = MockCacheRepo()
    vector_repo = MockVectorRepo()
    cache_service = TwoTierCacheService(cache_repo=cache_repo, vector_repo=vector_repo)

    service = PropertyService(
        property_repo=prop_repo,
        vector_repo=vector_repo,
        cache_service=cache_service,
        embedder=None
    )

    # 1. Create property
    new_prop = PropertyCreate(
        title="شقة فاخرة على البحر في الإسكندرية",
        location="Alexandria, Stanley",
        city="alexandria",
        district="ستانلي",
        listing_type="Sale",
        property_type="Apartment",
        price_egp=5500000.0,
        bedrooms=3,
        bathrooms=2,
        area_sqm=180.0,
        description="إطلالة بانورامية مباشرة على البحر المتوسط تشطيب الترا سوبر لوكس",
        url="https://aqarmap.com.eg/ar/listing/12345"
    )

    created = await service.create_property(new_prop)
    assert created.id is not None
    assert created.title == new_prop.title
    assert created.price_egp == 5500000.0

    # 2. Retrieve property
    fetched = await service.get_property(created.id)
    assert fetched is not None
    assert fetched.id == created.id

    # 3. Update property
    update_data = PropertyUpdate(price_egp=5200000.0)
    updated = await service.update_property(created.id, update_data)
    assert updated is not None
    assert updated.price_egp == 5200000.0

    # 4. Delete property
    deleted = await service.delete_property(created.id)
    assert deleted is True

    # 5. Verify deleted
    assert await service.get_property(created.id) is None


def test_rbac_property_endpoints(client, admin_headers, agent_headers, user_headers):
    prop_payload = {
        "title": "شقة للبيع في سموحة",
        "location": "Alexandria, Smouha",
        "city": "alexandria",
        "district": "سموحة",
        "listing_type": "Sale",
        "property_type": "Apartment",
        "price_egp": 2800000.0,
        "bedrooms": 3,
        "bathrooms": 2,
        "area_sqm": 150.0,
        "description": "تشطيب سوبر لوكس",
        "url": "https://aqarmap.com.eg/test/smouha-1"
    }

    # 1. Anonymous user cannot create property (401)
    unauth_resp = client.post("/api/v1/properties", json=prop_payload)
    assert unauth_resp.status_code == 401

    # 2. Regular user cannot create property (403 Forbidden - requires agent or admin)
    user_resp = client.post("/api/v1/properties", json=prop_payload, headers=user_headers)
    assert user_resp.status_code == 403

    # 3. Agent CAN create property (201 Created)
    agent_resp = client.post("/api/v1/properties", json=prop_payload, headers=agent_headers)
    assert agent_resp.status_code == 201
    created_id = agent_resp.json()["id"]

    # 4. Agent CANNOT delete property (403 Forbidden - deletion strictly requires admin)
    agent_del_resp = client.delete(f"/api/v1/properties/{created_id}", headers=agent_headers)
    assert agent_del_resp.status_code == 403

    # 5. Admin CAN delete property (204 No Content)
    admin_del_resp = client.delete(f"/api/v1/properties/{created_id}", headers=admin_headers)
    assert admin_del_resp.status_code == 204
