"""Main Dagster definitions for real estate pipeline"""
from dagster import (
    Definitions,
    define_asset_job,
    ScheduleDefinition,
    AssetSelection
)

# Import assets
from assets.scraping.scraping_assets import (
    scraping_assets,
    get_scraping_asset_names
)

from assets.mart.mart_assets import (
    mart_assets,
    get_mart_asset_names
)

from assets.vectors.vector_assets import process_to_milvus

from assets.summary.summary_assets import scraping_summary, mart_transformation_summary, complete_pipeline_summary

# Import resources
from resources.config_resources import (
    ScraperResource,
    MartResource,
    VectorResource
)

# Get all scraping asset names dynamically
scraping_asset_names = get_scraping_asset_names()
mart_asset_names = get_mart_asset_names()

# DEFINE JOBS
# Complete pipeline job
complete_pipeline_job = define_asset_job(
    name="complete_real_estate_pipeline",
    description="Full pipeline: Scraping → Mart → Vectors",
    selection=AssetSelection.keys(
        # Scraping
        *scraping_asset_names,
        "scraping_summary",

        # Mart transformation
        *mart_asset_names,
        "mart_transformation_summary",
        
        # Vector processing
        "process_to_milvus",

        # Final summary
        "complete_pipeline_summary"
    
))

# Scraping only job
scraping_only_job = define_asset_job(
    name="scraping_only",
    description="Only scraping job without transformation or vector processing",
    selection=AssetSelection.keys(
        *scraping_asset_names,
        "scraping_summary"
    )
)

# Mart transformation only job
mart_transformation_only_job = define_asset_job(
    name="mart_transformation_only",
    description="Only transform raw data to mart and update summary tables",
    selection=AssetSelection.keys(
        *mart_asset_names,
        "mart_transformation_summary"
    )
)

# Vector processing only job
vector_processing_only_job = define_asset_job(
    name="vector_processing_only",
    description="Only process Mart data to Milvus",
    selection=AssetSelection.keys("process_to_milvus")
)


# SCHEDULES
# Main schedule - runs complete pipeline daily at 12 PM
daily_complete_pipeline_schedule = ScheduleDefinition(
    name="daily_complete_pipeline_at_noon",
    job=complete_pipeline_job,
    cron_schedule="0 12 * * *",
    execution_timezone="Africa/Cairo",
    description="Runs complete pipeline daily at 12:00 PM Cairo time"
)

# Mart transformation schedule
mart_transformation_schedule = ScheduleDefinition(
    name="mart_transformation_at_2pm",
    job=mart_transformation_only_job,
    cron_schedule="0 14 * * *",
    execution_timezone="Africa/Cairo",
    description="Transforms raw data to mart at 2:00 PM Cairo time"
)

# Vector sync schedule
vector_sync_schedule = ScheduleDefinition(
    name="vector_sync_every_6_hours",
    job=vector_processing_only_job,
    cron_schedule="0 */6 * * *",
    execution_timezone="Africa/Cairo",
    description="Syncs Mart data to Milvus every 6 hours"
)


# DAGSTER DEFINITIONS
defs = Definitions(
    assets=[
        # Scraping assets
        *scraping_assets,
        scraping_summary,

        # Mart transformation assets
        *mart_assets,
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