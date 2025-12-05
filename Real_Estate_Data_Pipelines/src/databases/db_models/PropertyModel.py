# src/models/property.py
from pydantic import BaseModel
from typing import Optional
from google.cloud import bigquery

class PropertyModel(BaseModel):
    property_id: str
    source: Optional[str]
    url: Optional[str]
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
    scraped_at: Optional[str]
    loaded_at: str


# Define schema matching scraper data structure
PropertySchema = [
            bigquery.SchemaField("property_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("source", "STRING"),
            bigquery.SchemaField("url", "STRING"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("description", "STRING"),
            bigquery.SchemaField("price_egp", "FLOAT"),
            bigquery.SchemaField("price_text", "STRING"),
            bigquery.SchemaField("currency", "STRING"),
            bigquery.SchemaField("property_type", "STRING"),
            bigquery.SchemaField("listing_type", "STRING"),
            
            # Property details
            bigquery.SchemaField("bedrooms", "INTEGER"),
            bigquery.SchemaField("bathrooms", "INTEGER"),
            bigquery.SchemaField("area_sqm", "FLOAT"),
            bigquery.SchemaField("floor_number", "INTEGER"),
            
            # Location details
            bigquery.SchemaField("location", "STRING"),
            bigquery.SchemaField("address", "STRING"),
            bigquery.SchemaField("latitude", "FLOAT"),
            bigquery.SchemaField("longitude", "FLOAT"),
            bigquery.SchemaField("last_updated", "STRING"),
            
            # Images (as JSON string for simplicity)
            bigquery.SchemaField("images", "STRING"),
            bigquery.SchemaField("image_count", "INTEGER"),
            
            # Agent information
            bigquery.SchemaField("agent_type", "STRING"),
            
            # Timestamps
            bigquery.SchemaField("scraped_at", "TIMESTAMP"),
            bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
        ]