from pydantic import BaseModel, Field, field_validator
from typing import Optional

class PropertyModel(BaseModel):
    """Raw scraped data - lenient validation for initial ingestion"""
    
    # Required identity fields
    property_id: str = Field(
        ..., 
        min_length=18, 
        max_length=28,
        pattern="^[a-z]+_[a-f0-9]{16}$",
        description="Format: source_hash (e.g., aqarmap_48da83f859986cdb)"
    )
    source: str = Field(..., min_length=1, max_length=200)
    url: str = Field(..., min_length=1, max_length=500)
    location: str = Field(..., min_length=1, max_length=200)
    
    # Optional text fields
    title: Optional[str] = Field(None, max_length=500)
    description: Optional[str] = Field(None, max_length=65535)
    address: Optional[str] = Field(None, max_length=500)
    
    # Optional numeric fields
    price_egp: Optional[float] = Field(None, gt=0)
    price_text: Optional[str] = Field(None, max_length=100)
    currency: Optional[str] = Field(None, max_length=10)
    
    # Optional categorical fields
    property_type: Optional[str] = Field(None, max_length=100)
    listing_type: Optional[str] = Field(None, max_length=50)
    
    # Property details (optional)
    bedrooms: Optional[int] = Field(None, ge=0)
    bathrooms: Optional[int] = Field(None, ge=0)
    area_sqm: Optional[float] = Field(None, gt=0)
    floor_number: Optional[int] = None
    
    # Location coordinates (optional)
    latitude: Optional[float] = Field(None, ge=-90, le=90)
    longitude: Optional[float] = Field(None, ge=-180, le=180)
    last_updated: Optional[str] = None
    
    # Images
    images: Optional[str] = None  # JSON string
    image_count: Optional[int] = Field(None, ge=0)
    
    # Agent info
    agent_type: Optional[str] = Field(None, max_length=100)
    
    # Timestamps
    scraped_at: str
    loaded_at: str
    
    @field_validator('property_id')
    @classmethod
    def validate_property_id_format(cls, v):
        """Validate source_hash format"""
        if '_' not in v:
            raise ValueError("property_id must contain underscore separator")
        
        source, hash_part = v.rsplit('_', 1)
        
        if not source or len(source) < 2:
            raise ValueError("Source prefix must be at least 2 characters")
        
        if len(hash_part) != 16:
            raise ValueError(f"Hash must be 16 characters, got {len(hash_part)}")
        
        if not all(c in '0123456789abcdef' for c in hash_part):
            raise ValueError("Hash must be lowercase hexadecimal")
        
        return v
    
    @field_validator('property_id', 'url', 'location', 'source')
    @classmethod
    def validate_required_strings(cls, v):
        """Ensure required strings are not empty"""
        if not v or not v.strip():
            raise ValueError("Field cannot be empty")
        return v.strip()
    
    @field_validator('title', 'description', 'address', 'price_text', 
                     'currency', 'property_type', 'listing_type', 'agent_type')
    @classmethod
    def strip_optional_strings(cls, v):
        """Strip whitespace from optional string fields"""
        return v.strip() if v else None
    
    class Config:
        str_strip_whitespace = True