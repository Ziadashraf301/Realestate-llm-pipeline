from google.cloud import bigquery

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