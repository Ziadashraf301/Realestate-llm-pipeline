"""Scraping assets - Fully dynamic generation"""
from datetime import datetime
from typing import Dict, Any, List
from dagster import asset, OpExecutionContext, RetryPolicy
from src.config import config
from ...resources.config_resources import ScraperResource
from .scraping_config import SCRAPING_CONFIG


def scrape_city_listing(
    context: OpExecutionContext,
    scraper_resource: ScraperResource,
    city: str,
    listing_type: str
) -> Dict[str, Any]:
    """
    Generic scraping function for any city and listing type
    """
    try:
        context.log.info(f"🏠 Starting {city.title()} {listing_type} scraping...")
        
        # Import modules
        from src.scrapers import AQARMAPRealEstateScraper
        from src.databases import Big_Query_Database
        from src.helpers import save_to_json, scraper_report
        from src.logger import LoggerFactory
        
        # Initialize logger
        logger = LoggerFactory.create_logger(log_dir=scraper_resource.log_dir)
        
        # Initialize database
        db = Big_Query_Database(
            project_id=scraper_resource.project_id,
            raw_dataset_id=scraper_resource.raw_dataset_id,
            raw_table_id=scraper_resource.raw_table_id,
            log_dir=scraper_resource.log_dir
        )
        db.connect()
        
        # Initialize scraper
        scraper = AQARMAPRealEstateScraper(
            log_dir=scraper_resource.log_dir,
            db=db
        )
        
        # Scrape data
        results = scraper.scrape_aqarmap(
            city=city,
            listing_type=listing_type,
            max_pages=scraper_resource.max_pages
        )
        
        # Generate report
        scraper_report(results=results, logger=logger)
        context.log.info(f"✅ Scraped {len(results)} properties from {city.title()} ({listing_type})")
        
        # Save to BigQuery
        inserted_count = db.save_to_database(results)
        context.log.info(f"📤 Inserted {inserted_count} new properties to BigQuery")
        
        # Save to JSON
        filename = f"{city}_{listing_type.replace('-', '_')}.json"
        output_path = config.PROJECT_ROOT / "Real_Estate_Data_Pipelines" / "raw_data" / filename
        save_to_json(filename=str(output_path), results=results, logger=logger)
        
        # At the end, return with metadata
        from dagster import Output, MetadataValue
        
        return Output(
            value={
                "city": city,
                "listing_type": listing_type,
                "scraped_count": len(results),
                "inserted_count": inserted_count,
                "timestamp": datetime.now().isoformat(),
                "status": "success"
            },
            metadata={
                "scraped_count": MetadataValue.int(len(results)),
                "inserted_count": MetadataValue.int(inserted_count),
                "status": MetadataValue.text("success"),
                "city": MetadataValue.text(city),
                "listing_type": MetadataValue.text(listing_type)
            }
        )
        
    except Exception as e:
        context.log.error(f"❌ Error scraping {city.title()} {listing_type}: {str(e)}")
        
        from dagster import Output, MetadataValue
        
        return Output(
            value={
                "city": city,
                "listing_type": listing_type,
                "scraped_count": 0,
                "inserted_count": 0,
                "timestamp": datetime.now().isoformat(),
                "status": "failed",
                "error": str(e)
            },
            metadata={
                "scraped_count": MetadataValue.int(0),
                "inserted_count": MetadataValue.int(0),
                "status": MetadataValue.text("failed"),
                "error": MetadataValue.text(str(e))
            }
        )


def create_scraping_asset(city: str, listing_type: str):
    """
    Factory function to dynamically create scraping assets
    
    Args:
        city: City name
        listing_type: Listing type
    
    Returns:
        Dagster asset function
    """
    asset_name = f"scrape_{city}_{listing_type.replace('-', '_')}"
    
    @asset(
        name=asset_name,
        description=f"Scrape {city.title()} properties {listing_type}",
        group_name="real_estate_scraping",
        retry_policy=RetryPolicy(max_retries=2, delay=300)
    )
    def scraping_asset(context: OpExecutionContext, scraper_resource: ScraperResource):
        return scrape_city_listing(context, scraper_resource, city, listing_type)
    
    return scraping_asset


def get_all_scraping_assets() -> List:
    """
    Dynamically generate all scraping assets from config
    
    Returns:
        List of all scraping asset functions
    """
    assets = []
    for scraping_config in SCRAPING_CONFIG:
        city = scraping_config["city"]
        listing_type = scraping_config["listing_type"]
        asset_func = create_scraping_asset(city, listing_type)
        assets.append(asset_func)
    
    return assets


def get_scraping_asset_names():
    """Get list of all scraping asset names"""
    return [str(asset.key.path[-1]) for asset in scraping_assets]

# Generate all assets dynamically
scraping_assets = get_all_scraping_assets()

# This allows to import them individually if needed
_asset_map = {asset.key.path[-1]: asset for asset in scraping_assets}

# Create module-level variables dynamically
for asset_name, asset_func in _asset_map.items():
    globals()[asset_name] = asset_func