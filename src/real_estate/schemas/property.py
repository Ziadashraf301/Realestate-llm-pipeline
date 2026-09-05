"""
Property Domain Schemas (Pydantic v2).
Strict validation for real estate listings, CRUD operations, and vector payloads.
"""

from datetime import datetime, timezone
from typing import Literal
from pydantic import BaseModel, Field, HttpUrl, ConfigDict


class PropertyBase(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    title: str = Field(..., min_length=3, max_length=300, description="Listing title in Arabic or English")
    location: str = Field(..., description="Mapped city/district (e.g. 'alexandria', 'cairo', 'سموحة')")
    city: str | None = Field(None, description="City name (e.g. 'cairo', 'alexandria', 'giza')")
    district: str | None = Field(None, description="District / neighborhood name (e.g. 'smouha', 'tagamoa')")
    listing_type: Literal["تمليك", "ايجار", "Sale", "Rent"] = Field(..., description="Sale or Rent classification")
    property_type: str = Field(..., description="Apartment, Villa, Duplex, Chalet, etc.")
    price_egp: float = Field(..., ge=0, description="Price in Egyptian Pounds (EGP)")
    bedrooms: int | None = Field(None, ge=0, le=50, description="Number of bedrooms")
    bathrooms: int | None = Field(None, ge=0, le=20, description="Number of bathrooms")
    area_sqm: float | None = Field(None, ge=0, le=100000, description="Total property area in square meters")
    description: str | None = Field(None, max_length=5000, description="Detailed property description")
    url: str | None = Field(None, description="Original source listing URL")


class PropertyCreate(PropertyBase):
    pass


class PropertyUpdate(BaseModel):
    title: str | None = None
    location: str | None = None
    city: str | None = None
    district: str | None = None
    listing_type: Literal["تمليك", "ايجار", "Sale", "Rent"] | None = None
    property_type: str | None = None
    price_egp: float | None = Field(None, gt=0)
    bedrooms: int | None = Field(None, ge=0)
    bathrooms: int | None = Field(None, ge=0)
    area_sqm: float | None = Field(None, gt=0)
    description: str | None = None
    url: str | None = None


class PropertyRead(PropertyBase):
    id: str = Field(..., description="Unique property identifier")
    created_at: datetime | None = Field(default_factory=lambda: datetime.now(timezone.utc))
    similarity: float | None = Field(None, ge=0.0, le=1.0, description="Cosine similarity score from vector search")
