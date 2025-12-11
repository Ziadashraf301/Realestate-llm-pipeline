"""Scraping assets for real estate pipeline - using existing helpers"""
from datetime import datetime
from pathlib import Path
from dagster import asset, OpExecutionContext, RetryPolicy
from ...config.settings import config
from ...resources.config_resources import ScraperResource


@asset(
    description="Scrape Alexandria properties for sale",
    group_name="real_estate_scraping",
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def scrape_alexandria_for_sale(context: OpExecutionContext, scraper_resource: ScraperResource):
    """Scrape Alexandria properties for sale using existing helpers"""
    try:
        context.log.info("🏠 Starting Alexandria for-sale scraping...")
        
        # Import modules
        from src.scrapers.aqarmap.aqarmap_real_estate_scraper import AQARMAPRealEstateScraper
        from src.databases.big_query.big_query_database import Big_Query_Database
        from src.helpers.helpers import save_to_json, scraper_report
        from src.logger.logger_factory import LoggerFactory
        
        # Initialize logger using LoggerFactory
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
            city='alexandria',
            listing_type='for-sale',
            max_pages=scraper_resource.max_pages
        )
        
        # Generate report
        scraper_report(results=results, logger=logger)
        
        context.log.info(f"✅ Scraped {len(results)} properties from Alexandria (for-sale)")
        
        # Save to BigQuery
        inserted_count = db.save_to_database(results)
        context.log.info(f"📤 Inserted {inserted_count} new properties to BigQuery")
        
        # Save to JSON
        output_path = config.PROJECT_ROOT / "raw_data" / "alexandria_for_sale.json"
        save_to_json(filename=str(output_path), results=results, logger=logger)
        
        return {
            "city": "alexandria",
            "listing_type": "for-sale",
            "scraped_count": len(results),
            "inserted_count": inserted_count,
            "timestamp": datetime.now().isoformat(),
            "status": "success"
        }
        
    except Exception as e:
        context.log.error(f"❌ Error scraping Alexandria for-sale: {str(e)}")
        return {
            "city": "alexandria",
            "listing_type": "for-sale",
            "scraped_count": 0,
            "inserted_count": 0,
            "timestamp": datetime.now().isoformat(),
            "status": "failed",
            "error": str(e)
        }


@asset(
    description="Scrape Alexandria properties for rent",
    group_name="real_estate_scraping",
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def scrape_alexandria_for_rent(context: OpExecutionContext, scraper_resource: ScraperResource):
    """Scrape Alexandria properties for rent using existing helpers"""
    try:
        context.log.info("🏠 Starting Alexandria for-rent scraping...")
        
        from src.scrapers.aqarmap.aqarmap_real_estate_scraper import AQARMAPRealEstateScraper
        from src.databases.big_query.big_query_database import Big_Query_Database
        from src.helpers.helpers import save_to_json, scraper_report
        from src.logger.logger_factory import LoggerFactory
        
        logger = LoggerFactory.create_logger(log_dir=scraper_resource.log_dir)
        
        db = Big_Query_Database(
            project_id=scraper_resource.project_id,
            raw_dataset_id=scraper_resource.raw_dataset_id,
            raw_table_id=scraper_resource.raw_table_id,
            log_dir=scraper_resource.log_dir
        )
        db.connect()
        
        scraper = AQARMAPRealEstateScraper(
            log_dir=scraper_resource.log_dir,
            db=db
        )
        
        results = scraper.scrape_aqarmap(
            city='alexandria',
            listing_type='for-rent',
            max_pages=scraper_resource.max_pages
        )
        
        scraper_report(results=results, logger=logger)
        context.log.info(f"✅ Scraped {len(results)} properties from Alexandria (for-rent)")
        
        inserted_count = db.save_to_database(results)
        context.log.info(f"📤 Inserted {inserted_count} new properties to BigQuery")
        
        output_path = config.PROJECT_ROOT / "raw_data" / "alexandria_for_rent.json"
        save_to_json(filename=str(output_path), results=results, logger=logger)
        
        return {
            "city": "alexandria",
            "listing_type": "for-rent",
            "scraped_count": len(results),
            "inserted_count": inserted_count,
            "timestamp": datetime.now().isoformat(),
            "status": "success"
        }
        
    except Exception as e:
        context.log.error(f"❌ Error scraping Alexandria for-rent: {str(e)}")
        return {
            "city": "alexandria",
            "listing_type": "for-rent",
            "scraped_count": 0,
            "inserted_count": 0,
            "timestamp": datetime.now().isoformat(),
            "status": "failed",
            "error": str(e)
        }


@asset(
    description="Scrape Cairo properties for sale",
    group_name="real_estate_scraping",
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def scrape_cairo_for_sale(context: OpExecutionContext, scraper_resource: ScraperResource):
    """Scrape Cairo properties for sale using existing helpers"""
    try:
        context.log.info("🏠 Starting Cairo for-sale scraping...")
        
        from src.scrapers.aqarmap.aqarmap_real_estate_scraper import AQARMAPRealEstateScraper
        from src.databases.big_query.big_query_database import Big_Query_Database
        from src.helpers.helpers import save_to_json, scraper_report
        from src.logger.logger_factory import LoggerFactory
        
        logger = LoggerFactory.create_logger(log_dir=scraper_resource.log_dir)
        
        db = Big_Query_Database(
            project_id=scraper_resource.project_id,
            raw_dataset_id=scraper_resource.raw_dataset_id,
            raw_table_id=scraper_resource.raw_table_id,
            log_dir=scraper_resource.log_dir
        )
        db.connect()
        
        scraper = AQARMAPRealEstateScraper(
            log_dir=scraper_resource.log_dir,
            db=db
        )
        
        results = scraper.scrape_aqarmap(
            city='cairo',
            listing_type='for-sale',
            max_pages=scraper_resource.max_pages
        )
        
        scraper_report(results=results, logger=logger)
        context.log.info(f"✅ Scraped {len(results)} properties from Cairo (for-sale)")
        
        inserted_count = db.save_to_database(results)
        context.log.info(f"📤 Inserted {inserted_count} new properties to BigQuery")
        
        output_path = config.PROJECT_ROOT / "raw_data" / "cairo_for_sale.json"
        save_to_json(filename=str(output_path), results=results, logger=logger)
        
        return {
            "city": "cairo",
            "listing_type": "for-sale",
            "scraped_count": len(results),
            "inserted_count": inserted_count,
            "timestamp": datetime.now().isoformat(),
            "status": "success"
        }
        
    except Exception as e:
        context.log.error(f"❌ Error scraping Cairo for-sale: {str(e)}")
        return {
            "city": "cairo",
            "listing_type": "for-sale",
            "scraped_count": 0,
            "inserted_count": 0,
            "timestamp": datetime.now().isoformat(),
            "status": "failed",
            "error": str(e)
        }


@asset(
    description="Scrape Cairo properties for rent",
    group_name="real_estate_scraping",
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def scrape_cairo_for_rent(context: OpExecutionContext, scraper_resource: ScraperResource):
    """Scrape Cairo properties for rent using existing helpers"""
    try:
        context.log.info("🏠 Starting Cairo for-rent scraping...")
        
        from src.scrapers.aqarmap.aqarmap_real_estate_scraper import AQARMAPRealEstateScraper
        from src.databases.big_query.big_query_database import Big_Query_Database
        from src.helpers.helpers import save_to_json, scraper_report
        from src.logger.logger_factory import LoggerFactory
        
        logger = LoggerFactory.create_logger(log_dir=scraper_resource.log_dir)
        
        db = Big_Query_Database(
            project_id=scraper_resource.project_id,
            raw_dataset_id=scraper_resource.raw_dataset_id,
            raw_table_id=scraper_resource.raw_table_id,
            log_dir=scraper_resource.log_dir
        )
        db.connect()
        
        scraper = AQARMAPRealEstateScraper(
            log_dir=scraper_resource.log_dir,
            db=db
        )
        
        results = scraper.scrape_aqarmap(
            city='cairo',
            listing_type='for-rent',
            max_pages=scraper_resource.max_pages
        )
        
        scraper_report(results=results, logger=logger)
        context.log.info(f"✅ Scraped {len(results)} properties from Cairo (for-rent)")
        
        inserted_count = db.save_to_database(results)
        context.log.info(f"📤 Inserted {inserted_count} new properties to BigQuery")
        
        output_path = config.PROJECT_ROOT / "raw_data" / "cairo_for_rent.json"
        save_to_json(filename=str(output_path), results=results, logger=logger)
        
        return {
            "city": "cairo",
            "listing_type": "for-rent",
            "scraped_count": len(results),
            "inserted_count": inserted_count,
            "timestamp": datetime.now().isoformat(),
            "status": "success"
        }
        
    except Exception as e:
        context.log.error(f"❌ Error scraping Cairo for-rent: {str(e)}")
        return {
            "city": "cairo",
            "listing_type": "for-rent",
            "scraped_count": 0,
            "inserted_count": 0,
            "timestamp": datetime.now().isoformat(),
            "status": "failed",
            "error": str(e)
        }