"""
Property Management Endpoints (FastAPI Controller Layer).
Delegates all business logic, vector synchronization, and cache invalidation to PropertyService.
Adheres strictly to Clean Architecture and Role-Based Access Control (RBAC).
"""

from typing import Annotated, List
from fastapi import APIRouter, Depends, HTTPException, status
from real_estate.api.deps import (
    get_property_service,
    require_roles,
)
from real_estate.services.property_service import PropertyService
from real_estate.schemas.property import PropertyRead, PropertyCreate, PropertyUpdate
from real_estate.schemas.auth import TokenPayload

router = APIRouter(prefix="/properties", tags=["Property Management (CRUD)"])


@router.get("", response_model=List[PropertyRead], summary="List Properties")
async def list_properties(
    property_service: Annotated[PropertyService, Depends(get_property_service)],
    limit: int = 20,
    offset: int = 0,
):
    """Lists properties with pagination (Public read access)."""
    return await property_service.list_properties(limit=limit, offset=offset)


@router.get("/{property_id}", response_model=PropertyRead, summary="Get Property by ID")
async def get_property(
    property_id: str,
    property_service: Annotated[PropertyService, Depends(get_property_service)],
):
    """Retrieves single property by ID (Public read access)."""
    prop = await property_service.get_property(property_id)
    if not prop:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Property not found")
    return prop


@router.post("", response_model=PropertyRead, status_code=status.HTTP_201_CREATED, summary="Create Property Listing")
async def create_property(
    property_in: PropertyCreate,
    current_user: Annotated[TokenPayload, Depends(require_roles("agent", "admin"))],
    property_service: Annotated[PropertyService, Depends(get_property_service)],
):
    """
    Creates new property, triggers dense ONNX embedding generation,
    indexes into Milvus vector collection, and invalidates two-tier search cache.
    RBAC: Requires 'agent' or 'admin' role.
    """
    return await property_service.create_property(property_in)


@router.put("/{property_id}", response_model=PropertyRead, summary="Update Property Listing")
async def update_property(
    property_id: str,
    property_in: PropertyUpdate,
    current_user: Annotated[TokenPayload, Depends(require_roles("agent", "admin"))],
    property_service: Annotated[PropertyService, Depends(get_property_service)],
):
    """
    Updates property metadata, synchronizes vectors, and invalidates cache.
    RBAC: Requires 'agent' or 'admin' role.
    """
    updated = await property_service.update_property(property_id, property_in)
    if not updated:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Property not found")
    return updated


@router.delete("/{property_id}", status_code=status.HTTP_204_NO_CONTENT, summary="Delete Property Listing")
async def delete_property(
    property_id: str,
    current_user: Annotated[TokenPayload, Depends(require_roles("admin"))],
    property_service: Annotated[PropertyService, Depends(get_property_service)],
):
    """
    Deletes property, removes vector from Milvus, and flushes search cache.
    RBAC: Strictly restricted to 'admin' role.
    """
    deleted = await property_service.delete_property(property_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Property not found")
    return None
