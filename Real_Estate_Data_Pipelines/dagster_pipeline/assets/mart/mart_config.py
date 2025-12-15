"""Configuration for mart transformation assets"""

# Import all scraping assets dynamically
from ..scraping.scraping_assets import scraping_assets

# Generate dynamic dependencies for scraping_summary
scraping_deps = [str(asset_def.key.path[-1]) for asset_def in scraping_assets]

# Define all mart configurations
MART_CONFIG = [
    {
        "asset_name": "property_mart",
        "description": "Transform raw data to property mart table",
        "group_name": "mart_transformation",
        "deps": scraping_deps,
        "mart_method": "create_mart_table"
    },
    {
        "asset_name": "location_summary",
        "description": "Location-based summary aggregations",
        "group_name": "mart_summaries",
        "deps": ["property_mart"],
        "mart_method": "create_location_summary_mart"
    },
    {
        "asset_name": "property_type_summary",
        "description": "Property type summary aggregations",
        "group_name": "mart_summaries",
        "deps": ["property_mart"],
        "mart_method": "create_property_type_summary_mart"
    },
    {
        "asset_name": "time_series_summary",
        "description": "Time series summary aggregations",
        "group_name": "mart_summaries",
        "deps": ["property_mart"],
        "mart_method": "create_time_series_summary_mart"
    },
    {
        "asset_name": "price_analysis_summary",
        "description": "Price analysis summary aggregations",
        "group_name": "mart_summaries",
        "deps": ["property_mart"],
        "mart_method": "create_price_analysis_summary_mart"
    },
    {
        "asset_name": "data_quality_report",
        "description": "Data quality report",
        "group_name": "mart_summaries",
        "deps": ["property_mart"],
        "mart_method": "create_data_quality_report_mart"
    }
]