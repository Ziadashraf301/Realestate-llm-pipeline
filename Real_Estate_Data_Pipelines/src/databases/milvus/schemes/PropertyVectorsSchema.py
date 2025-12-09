from pymilvus import DataType

def get_property_schema(embedding_dim=768):
    """Factory function to create schema with dynamic embedding_dim"""
    return {
        "fields": [
            {"field_name": "property_id", "datatype": DataType.VARCHAR, 
             "is_primary": True, "max_length": 50},
            {"field_name": "embedding", "datatype": DataType.FLOAT_VECTOR, 
             "dim": embedding_dim},
            {"field_name": "text", "datatype": DataType.VARCHAR, "max_length": 65535},
            {"field_name": "source", "datatype": DataType.VARCHAR, "max_length": 200},
            {"field_name": "url", "datatype": DataType.VARCHAR, "max_length": 500},
            {"field_name": "title", "datatype": DataType.VARCHAR, "max_length": 500},
            {"field_name": "property_type", "datatype": DataType.VARCHAR, "max_length": 100},
            {"field_name": "listing_type", "datatype": DataType.VARCHAR, "max_length": 50},
            {"field_name": "location", "datatype": DataType.VARCHAR, "max_length": 200},
            {"field_name": "price_egp", "datatype": DataType.FLOAT},
            {"field_name": "bedrooms", "datatype": DataType.INT64},
            {"field_name": "bathrooms", "datatype": DataType.INT64},
            {"field_name": "area_sqm", "datatype": DataType.FLOAT},
            {"field_name": "floor_number", "datatype": DataType.INT64},
            {"field_name": "latitude", "datatype": DataType.FLOAT},
            {"field_name": "longitude", "datatype": DataType.FLOAT},
        ]
    }