from pydantic import BaseModel, Field, field_validator, model_validator
from typing import List, Optional

class PropertyVectorsModel(BaseModel):
    """Validated model for VectorDB insertion - STRICT requirements"""
    
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
    location: str = Field(..., min_length=2, max_length=200)
    
    # Required vector field
    embedding: List[float] = Field(..., min_length=384, max_length=384*4)
    
    # Required text fields (strict length requirements)
    text: str = Field(..., min_length=10, max_length=12000)
    title: str = Field(..., min_length=3, max_length=500)
    
    # Required categorical fields (strict validation)
    property_type: str = Field(..., min_length=1, max_length=100)
    listing_type: str = Field(..., max_length=50)
    
    # Required numeric fields (must exist and be in valid range)
    price_egp: float = Field(..., gt=1000, lt=1_000_000_000)  # At least 1000 EGP
    bedrooms: int = Field(..., ge=0, le=25)
    bathrooms: int = Field(..., ge=0, le=15)
    area_sqm: float = Field(..., gt=9, le=10000)
    
    # Optional but validated if present
    floor_number: Optional[int] = Field(default=0, ge=-2, le=100)
    latitude: Optional[float] = Field(None, ge=22.0, le=32.0)  # Egypt bounds
    longitude: Optional[float] = Field(None, ge=25.0, le=37.0)  # Egypt bounds
    
    # Validators
    @field_validator('property_id')
    @classmethod
    def validate_property_id_format(cls, v):
        """Validate source_hash format (same as PropertyModel)"""
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
    
    @field_validator('property_id', 'url', 'location', 'source', 'text', 'title', 'property_type')
    @classmethod
    def validate_required_strings(cls, v):
        """Ensure required strings are not empty (same as PropertyModel)"""
        if not v or not v.strip():
            raise ValueError("Field cannot be empty")
        return v.strip()
    
    @field_validator('embedding')
    @classmethod
    def validate_embedding(cls, v):
        """Ensure embedding is valid"""
        if len(v) < 384 or len(v) > 384*4:
            raise ValueError(f"Embedding must be exactly 384 dimensions, got {len(v)}")
        
        if not all(isinstance(x, (int, float)) for x in v):
            raise ValueError("Embedding must contain only numbers")
        
        # Check for zero vector
        magnitude = sum(x**2 for x in v) ** 0.5
        if magnitude == 0:
            raise ValueError("Embedding cannot be zero vector")
        
        return v
    
    class Config:
        str_strip_whitespace = True
        validate_assignment = True