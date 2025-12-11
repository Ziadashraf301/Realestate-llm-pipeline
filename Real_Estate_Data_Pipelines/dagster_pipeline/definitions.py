"""Main Dagster definitions for real estate pipeline"""
from dagster import (
    Definitions,
    define_asset_job,
    ScheduleDefinition
)

# Import assets
from .assets.scraping.scraping_assets import (
    scrape_alexandria_for_sale,
    scrape_alexandria_for_rent,
    scrape_cairo_for_sale,
    scrape_cairo_for_rent
)

from .assets.mart.mart_assets import (
    property_mart,
    location_summary,
    property_type_summary,
    time_series_summary,
    price_analysis_summary,
    data_quality_report
)

from .assets.vectors.vector_assets import process_to_milvus

from .assets.summary.summary_assets import scraping_summary, mart_transformation_summary, complete_pipeline_summary

# Import resources
from .resources.config_resources import (
    ScraperResource,
    MartResource,
    VectorResource
)

from .config.settings import config


# DEFINE JOBS

# Complete pipeline job
complete_pipeline_job = define_asset_job(
    name="complete_real_estate_pipeline",
    description="Full pipeline: Scraping → Mart → Vectors",
    selection=[
        # Scraping
        "scrape_alexandria_for_sale",
        "scrape_alexandria_for_rent",
        "scrape_cairo_for_sale",
        "scrape_cairo_for_rent",
        "scraping_summary",

        # Mart transformation
        "property_mart",
        "location_summary",
        "property_type_summary",
        "time_series_summary",
        "price_analysis_summary",
        "data_quality_report",
        "mart_transformation_summary",

        # Vector processing
        "process_to_milvus",

        # Final summary
        "complete_pipeline_summary"
    ]
)

# Scraping only job
scraping_only_job = define_asset_job(
    name="scraping_only",
    description="Only scraping job without transformation or vector processing",
    selection=[
        "scrape_alexandria_for_sale",
        "scrape_alexandria_for_rent",
        "scrape_cairo_for_sale",
        "scrape_cairo_for_rent",
        "scraping_summary"
    ]
)

# Mart transformation only job
mart_transformation_only_job = define_asset_job(
    name="mart_transformation_only",
    description="Only transform raw data to mart and update summary tables",
    selection=[
        "property_mart",
        "location_summary",
        "property_type_summary",
        "time_series_summary",
        "price_analysis_summary",
        "data_quality_report",
        "mart_transformation_summary"
    ]
)

# Vector processing only job
vector_processing_only_job = define_asset_job(
    name="vector_processing_only",
    description="Only process Mart data to Milvus",
    selection=["process_to_milvus"]
)


# SCHEDULES
# Main schedule - runs complete pipeline daily at 12 PM
daily_complete_pipeline_schedule = ScheduleDefinition(
    name="daily_complete_pipeline_at_noon",
    job=complete_pipeline_job,
    cron_schedule="0 12 * * *",  # Every day at 12:00 PM
    execution_timezone="Africa/Cairo",
    description="Runs complete pipeline daily at 12:00 PM Cairo time"
)

# Mart transformation schedule
mart_transformation_schedule = ScheduleDefinition(
    name="mart_transformation_at_2pm",
    job=mart_transformation_only_job,
    cron_schedule="0 14 * * *",  # Every day at 2:00 PM
    execution_timezone="Africa/Cairo",
    description="Transforms raw data to mart at 2:00 PM Cairo time"
)

# Vector sync schedule
vector_sync_schedule = ScheduleDefinition(
    name="vector_sync_every_6_hours",
    job=vector_processing_only_job,
    cron_schedule="0 */6 * * *",  # Every 6 hours
    execution_timezone="Africa/Cairo",
    description="Syncs Mart data to Milvus every 6 hours"
)


# DAGSTER DEFINITIONS

defs = Definitions(
    assets=[
        # Scraping assets
        scrape_alexandria_for_sale,
        scrape_alexandria_for_rent,
        scrape_cairo_for_sale,
        scrape_cairo_for_rent,
        scraping_summary,

        # Mart transformation assets
        property_mart,
        location_summary,
        property_type_summary,
        time_series_summary,
        price_analysis_summary,
        data_quality_report,
        mart_transformation_summary,

        # Vector processing assets
        process_to_milvus,
        
        # Final summary
        complete_pipeline_summary
    ],
    jobs=[
        complete_pipeline_job,
        scraping_only_job,
        mart_transformation_only_job,
        vector_processing_only_job
    ],
    schedules=[
        daily_complete_pipeline_schedule,
        mart_transformation_schedule,  
        vector_sync_schedule  
    ],
    resources={
        "scraper_resource": ScraperResource(),
        "mart_resource": MartResource(),
        "vector_resource": VectorResource()
    }
)