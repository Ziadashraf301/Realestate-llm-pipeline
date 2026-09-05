"""Configuration for mart transformation assets (Clean Architecture)."""

from real_estate.pipelines.assets.scraping.scraping_assets import scraping_assets

# Generate dynamic dependencies for property_mart from all dynamic scraping assets
scraping_deps = [str(asset_def.key.path[-1]) for asset_def in scraping_assets]

MART_CONFIG = [
    {
        "asset_name": "property_mart",
        "description": "Transform raw data to deduplicated property mart in ClickHouse",
        "group_name": "mart_transformation",
        "deps": scraping_deps,
        "mart_method": "validate_property_mart"
    },
    {
        "asset_name": "location_summary",
        "description": "Location-based summary aggregations in ClickHouse",
        "group_name": "mart_summaries",
        "deps": ["property_mart"],
        "mart_method": "create_location_summary"
    },
    {
        "asset_name": "price_analysis_summary",
        "description": "Price and bedroom distribution summary",
        "group_name": "mart_summaries",
        "deps": ["property_mart"],
        "mart_method": "create_price_summary"
    },
    {
        "asset_name": "data_quality_report",
        "description": "Nullability and coordinate coverage health report",
        "group_name": "mart_summaries",
        "deps": ["property_mart"],
        "mart_method": "create_quality_report"
    }
]