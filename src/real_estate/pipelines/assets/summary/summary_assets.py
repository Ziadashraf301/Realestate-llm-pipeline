"""
Summary assets for real estate pipeline (Clean Architecture).
Aggregates telemetry across all dynamic scraping assets and mart transformations.
"""

from datetime import datetime
from typing import Dict, Any
from dagster import asset, OpExecutionContext, RetryPolicy, Output, MetadataValue

from real_estate.pipelines.assets.scraping.scraping_assets import get_scraping_asset_names
from real_estate.pipelines.assets.mart.mart_assets import get_mart_asset_names
from real_estate.repositories.cache_repository import RedisCacheRepository
import asyncio

scraping_deps = get_scraping_asset_names()
mart_deps = get_mart_asset_names()


@asset(
    description="Summary of all dynamic scraping operations across all cities and providers",
    group_name="real_estate_scraping",
    deps=scraping_deps,
    retry_policy=RetryPolicy(max_retries=2, delay=60)
)
def scraping_summary(context: OpExecutionContext) -> Output[Dict[str, Any]]:
    """Aggregates all dynamically generated scraping assets."""
    total_assets = len(scraping_deps)
    context.log.info(f"📊 Aggregating {total_assets} dynamic scraping assets...")

    return Output(
        value={
            "total_scraping_assets": total_assets,
            "asset_keys": scraping_deps,
            "timestamp": datetime.utcnow().isoformat(),
            "status": "completed"
        },
        metadata={
            "total_scraping_assets": MetadataValue.int(total_assets),
            "status": MetadataValue.text("completed")
        }
    )


@asset(
    description="Summary of all mart transformations in ClickHouse",
    group_name="mart_summaries",
    deps=mart_deps,
    retry_policy=RetryPolicy(max_retries=2, delay=60)
)
def mart_transformation_summary(context: OpExecutionContext) -> Output[Dict[str, Any]]:
    """Aggregates all mart transformation assets."""
    total_marts = len(mart_deps)
    context.log.info(f"📊 Aggregating {total_marts} mart transformation assets...")

    return Output(
        value={
            "total_mart_assets": total_marts,
            "mart_keys": mart_deps,
            "timestamp": datetime.utcnow().isoformat(),
            "status": "completed"
        },
        metadata={
            "total_mart_assets": MetadataValue.int(total_marts),
            "status": MetadataValue.text("completed")
        }
    )


@asset(
    description="Complete end-to-end pipeline summary and cache invalidation",
    group_name="pipeline_summary",
    deps=["process_to_milvus", "mart_transformation_summary"],
    retry_policy=RetryPolicy(max_retries=2, delay=60)
)
def complete_pipeline_summary(context: OpExecutionContext) -> Output[Dict[str, Any]]:
    """Final pipeline stage: flushes Tier-1 search cache and logs run telemetry."""
    context.log.info("🏁 Finalizing complete pipeline and invalidating stale Redis search cache...")

    cache_repo = RedisCacheRepository()
    asyncio.run(cache_repo.flush())

    return Output(
        value={
            "status": "success",
            "cache_invalidated": True,
            "timestamp": datetime.utcnow().isoformat()
        },
        metadata={
            "cache_status": MetadataValue.text("Invalidated Redis Tier-1 Cache"),
            "status": MetadataValue.text("SUCCESS")
        }
    )