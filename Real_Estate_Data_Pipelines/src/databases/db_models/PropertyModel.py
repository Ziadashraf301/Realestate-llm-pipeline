from pydantic import BaseModel
from typing import Optional

class PropertyModel(BaseModel):
    property_id: str
    source: str
    url: str
    title: Optional[str]
    description: Optional[str]
    price_egp: Optional[float]
    price_text: Optional[str]
    currency: Optional[str]
    property_type: Optional[str]
    listing_type: Optional[str]
    
    # Property details
    bedrooms: Optional[int]
    bathrooms: Optional[int]
    area_sqm: Optional[float]
    floor_number: Optional[int]
    
    # Location details
    location: Optional[str]
    address: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    last_updated: Optional[str]
    
    # Images
    images: Optional[str]  # JSON string
    image_count: Optional[int]
    
    # Agent info
    agent_type: Optional[str]
    
    # Timestamps
    scraped_at: str
    loaded_at: str