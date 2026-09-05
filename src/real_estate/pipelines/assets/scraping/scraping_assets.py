"""
Dynamic Scraping Assets Factory for Dagster (Clean Architecture).
Dynamically instantiates Dagster asset nodes for every combination of (provider, city, listing_type).
Adding any new city to CITIES automatically generates corresponding Dagster assets.
"""

import asyncio
from datetime import datetime
from typing import Any, Dict, List
from dagster import asset, OpExecutionContext, RetryPolicy, Output, MetadataValue

from real_estate.core.logger import logger
from real_estate.repositories.warehouse_repository import ClickHouseWarehouseRepository
from real_estate.repositories.cache_repository import RedisCacheRepository
from real_estate.ingestion.aqarmap import AsyncAQARMAPScraper
from real_estate.ingestion.bayut import AsyncBayutScraper
from real_estate.pipelines.resources.config_resources import ScraperResource
from .scraping_config import SCRAPING_CONFIG


def scrape_city_listing(
    context: OpExecutionContext,
    scraper_resource: ScraperResource,
    provider: str,
    city: str,
    listing_type: str
) -> Output[Dict[str, Any]]:
    """Generic execution function for any dynamically generated scraping asset."""
    context.log.info(f"🏠 Starting {provider} {city.title()} ({listing_type}) scraping...")

    warehouse = ClickHouseWarehouseRepository()
    cache_repo = RedisCacheRepository()

    async def _run():
        await warehouse.initialize()

        if provider.lower() == "aqarmap":
            scraper = AsyncAQARMAPScraper(
                warehouse=warehouse,
                cache_repo=cache_repo,
                max_concurrency=scraper_resource.max_concurrency,
                logger=context.log,
            )
        else:
            scraper = AsyncBayutScraper(
                warehouse=warehouse,
                cache_repo=cache_repo,
                max_concurrency=scraper_resource.max_concurrency,
                logger=context.log,
            )

        results = await scraper.scrape(
            city=city,
            listing_type=listing_type,
            max_pages=scraper_resource.max_pages
        )
        return results

    try:
        results = asyncio.run(_run())
        scraped_count = len(results)
        context.log.info(f"✅ Scraped and stored {scraped_count} properties for {provider} {city} ({listing_type}) in ClickHouse")

        return Output(
            value={
                "provider": provider,
                "city": city,
                "listing_type": listing_type,
                "scraped_count": scraped_count,
                "timestamp": datetime.utcnow().isoformat(),
                "status": "success"
            },
            metadata={
                "provider": MetadataValue.text(provider),
                "city": MetadataValue.text(city),
                "listing_type": MetadataValue.text(listing_type),
                "scraped_count": MetadataValue.int(scraped_count),
                "warehouse": MetadataValue.text("ClickHouse ReplacingMergeTree")
            }
        )
    except Exception as e:
        context.log.error(f"❌ Error scraping {provider} {city} ({listing_type}): {str(e)}")
        raise


def create_scraping_asset(provider: str, city: str, listing_type: str):
    """Factory function dynamically generating an isolated Dagster asset."""
    clean_listing = listing_type.replace("-", "_")
    asset_name = f"scrape_{provider}_{city}_{clean_listing}"

    @asset(
        name=asset_name,
        description=f"Scrapes {provider.title()} listings in {city.title()} for {listing_type}",
        group_name="real_estate_scraping",
        retry_policy=RetryPolicy(max_retries=2, delay=120)
    )
    def _dynamic_asset(context: OpExecutionContext, scraper_resource: ScraperResource):
        return scrape_city_listing(context, scraper_resource, provider, city, listing_type)

    return _dynamic_asset


def get_all_scraping_assets() -> List:
    """Instantiates and returns all dynamically registered scraping assets."""
    assets = []
    for cfg in SCRAPING_CONFIG:
        asset_func = create_scraping_asset(cfg["provider"], cfg["city"], cfg["listing_type"])
        assets.append(asset_func)
    return assets


scraping_assets = get_all_scraping_assets()


def get_scraping_asset_names() -> List[str]:
    """Returns string asset keys for all dynamically generated scraping assets."""
    return [str(asset_def.key.path[-1]) for asset_def in scraping_assets]


# Register at module level for Dagster reflection
_asset_map = {asset_def.key.path[-1]: asset_def for asset_def in scraping_assets}
for _name, _func in _asset_map.items():
    globals()[_name] = _func