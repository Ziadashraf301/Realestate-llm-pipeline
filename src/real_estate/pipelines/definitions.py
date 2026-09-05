"""
Main Dagster Definitions for Real Estate Intelligence Pipeline (Clean Architecture M2).
Dynamically instantiates assets for every city/provider combination from config,
orchestrating Scraping -> ClickHouse ReplacingMergeTree -> Milvus HNSW -> Cache Invalidation.
"""

from dagster import (
    Definitions,
    define_asset_job,
    ScheduleDefinition,
    AssetSelection,
)

# 1. Dynamic Scraping Assets (Dynamic city combinations factory)
from real_estate.pipelines.assets.scraping.scraping_assets import (
    scraping_assets,
    get_scraping_asset_names,
)

# 2. Dynamic Mart Assets (ClickHouse deduplication & summaries)
from real_estate.pipelines.assets.mart.mart_assets import (
    mart_assets,
    get_mart_asset_names,
)

# 3. Vector Processing Asset (ONNX INT8 -> Milvus HNSW)
from real_estate.pipelines.assets.vectors.vector_assets import process_to_milvus

# 4. Summary & Invalidation Assets
from real_estate.pipelines.assets.summary.summary_assets import (
    scraping_summary,
    mart_transformation_summary,
    complete_pipeline_summary,
)

# 5. Resources
from real_estate.pipelines.resources.config_resources import (
    ScraperResource,
    MartResource,
    VectorResource,
)

# Dynamically discover all generated asset names
scraping_asset_names = get_scraping_asset_names()
mart_asset_names = get_mart_asset_names()


# =============================================================================
# DEFINE JOBS
# =============================================================================

# 1. Complete End-to-End Pipeline Job
complete_pipeline_job = define_asset_job(
    name="complete_real_estate_pipeline",
    description="Full dynamic pipeline: Dynamic City Scraping -> ClickHouse Mart -> Milvus Vectors -> Cache Flush",
    selection=AssetSelection.keys(
        *scraping_asset_names,
        "scraping_summary",
        *mart_asset_names,
        "mart_transformation_summary",
        "process_to_milvus",
        "complete_pipeline_summary",
    )
)

# 2. Scraping Only Job
scraping_only_job = define_asset_job(
    name="scraping_only",
    description="Runs all dynamic city scraping assets into ClickHouse without downstream processing",
    selection=AssetSelection.keys(
        *scraping_asset_names,
        "scraping_summary",
    )
)

# 3. Mart Transformation Only Job
mart_transformation_only_job = define_asset_job(
    name="mart_transformation_only",
    description="Transforms and validates raw ClickHouse data into deduplicated analytical marts",
    selection=AssetSelection.keys(
        *mart_asset_names,
        "mart_transformation_summary",
    )
)

# 4. Vector Processing Only Job
vector_processing_only_job = define_asset_job(
    name="vector_processing_only",
    description="Streams ClickHouse mart into Milvus dense vector collection with ONNX INT8 embeddings",
    selection=AssetSelection.keys(
        "process_to_milvus",
        "complete_pipeline_summary",
    )
)


# =============================================================================
# SCHEDULES
# =============================================================================

# Main Daily Schedule (2:00 AM Cairo Time)
daily_complete_pipeline_schedule = ScheduleDefinition(
    name="daily_complete_pipeline_at_2am",
    job=complete_pipeline_job,
    cron_schedule="0 2 * * *",
    execution_timezone="Africa/Cairo",
    description="Runs complete scraping, ClickHouse upsert, and Milvus vector indexing daily at 2:00 AM Cairo time"
)


# =============================================================================
# MASTER DEFINITIONS
# =============================================================================

defs = Definitions(
    assets=[
        *scraping_assets,
        scraping_summary,
        *mart_assets,
        mart_transformation_summary,
        process_to_milvus,
        complete_pipeline_summary,
    ],
    jobs=[
        complete_pipeline_job,
        scraping_only_job,
        mart_transformation_only_job,
        vector_processing_only_job,
    ],
    schedules=[
        daily_complete_pipeline_schedule,
    ],
    resources={
        "scraper_resource": ScraperResource(),
        "mart_resource": MartResource(),
        "vector_resource": VectorResource(),
    }
)