import os
from pathlib import Path
from src.scrapers import AQARMAPRealEstateScraper
import warnings
from src.config import Config
from src.databases import Big_Query_Database
from src.logger import LoggerFactory
from src.helpers import save_to_json, scraper_report

def test_scrapers_operations():
    """Main execution"""
    
    # Set Dir
    PROJECT_ROOT = Path(__file__).resolve().parents[3]
    CONFIG_DIR = PROJECT_ROOT / "Configs"
    CONFIG_PATH = CONFIG_DIR / "Real_Estate_Data_Pipelines.json"
    GOOGLE_APPLICATION_CREDENTIALS = CONFIG_DIR / "big_query_service_account.json"

    # Load Config
    cfg = Config(CONFIG_PATH)

    # Initialize logger
    logger = LoggerFactory.create_logger(log_dir=cfg.LOG_DIR)

    logger.info("""
    ╔══════════════════════════════════════════════════════════╗
    ║   Enhanced Egyptian Real Estate Scraper v2.0             ║
    ║   Features: URL tracking, logging, skip duplicates       ║
    ║   Storage: JSON + BigQuery                               ║
    ╚══════════════════════════════════════════════════════════╝
    """)

    # Suppress unnecessary warnings
    warnings.filterwarnings("ignore")

    # Set env variables dynamically
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(GOOGLE_APPLICATION_CREDENTIALS)
    os.environ["GCP_PROJECT_ID"] = cfg.GCP_PROJECT_ID
    os.environ["BQ_DATASET_ID"] = cfg.BQ_RAW_DATASET_ID
    os.environ["BQ_TABLE_ID"] = cfg.BQ_RAW_TABLE_ID
    json_file = 'C:/Users/MSI/OneDrive/Desktop/real_estate/Real_Estate_Data_Pipelines/raw_data/alexandria_for_sale.json'

    # Log
    logger.info(f"✅ Configuration loaded dynamically from:{CONFIG_PATH}")
    logger.info(f"GCP_PROJECT_ID: {cfg.GCP_PROJECT_ID}")
    logger.info(f"BQ_DATASET_ID: {cfg.BQ_RAW_DATASET_ID}")
    logger.info(f"BQ_TABLE_ID: {cfg.BQ_RAW_TABLE_ID}")

    bg_database = Big_Query_Database(
        project_id=os.environ["GCP_PROJECT_ID"],
        dataset_id=os.environ["BQ_DATASET_ID"],
        table_id=os.environ["BQ_TABLE_ID"],
        log_dir=cfg.LOG_DIR, 
    )

    scraper = AQARMAPRealEstateScraper(
        log_dir=cfg.LOG_DIR,
        db_client=bg_database
    )
    
    # Configuration
    CITY = 'alexandria'           # cairo, alexandria, giza, etc.
    LISTING_TYPE = 'for-sale'     # for-sale or for-rent
    MAX_PAGES = cfg.MAX_PAGES                # Number of pages to scrape
    
    logger.info(f"⚙️  Configuration:")
    logger.info(f"  • City: {CITY}")
    logger.info(f"  • Type: {LISTING_TYPE}")
    logger.info(f"  • Pages: {MAX_PAGES}")
    logger.info(f"  • Deep scraping: ENABLED")
    logger.info(f"  • URL tracking: ENABLED")
    logger.info(f"  • Logging: ENABLED")
    
    try:
        # Scrape
        logger.info("🔄 Starting scraping process...\n")
        scraper.scrape_aqarmap(
            city=CITY,
            listing_type=LISTING_TYPE,
            max_pages=MAX_PAGES
        )
        
        # print summary
        scraper_report(results=scraper.results, logger=logger)
        
        # Save results
        if scraper.results:
            logger.info("💾 Saving results...")
            
            # Save to JSON
            save_to_json(filename=json_file, results=scraper.results, logger=logger)
            logger.info(f"✅ Saved to JSON: {json_file}")
            
            # Save to BigQuery
            try:
                inserted_count = bg_database.save_to_database(scraper.results)
                
                if inserted_count > 0:
                    logger.info(f"✅ Uploaded {inserted_count} properties to BigQuery")
                else:
                    logger.info("ℹ️  No new properties uploaded to BigQuery (all were duplicates)")
                    
            except Exception as bq_error:
                logger.warning(f"⚠️  BigQuery upload failed: {bq_error}")
                logger.info("   Data is still saved in JSON format")
            
            logger.info("✅ Scraping completed successfully!")
            logger.info("📁 Files created:")
            logger.info(f"{json_file}")
            logger.info("logs/aqarmap_scraper.log (activity log)")
        else:
            logger.warning("⚠️  No new properties found to save")
        
    except KeyboardInterrupt:
        logger.warning("⚠️  Scraping interrupted by user")
        logger.info("💾  Saving partial results...")
        if scraper.results:
            save_to_json(filename=json_file, results=scraper.results, logger=logger)
            logger.info("✅ Partial results saved")
        
    except Exception as e:
        logger.error(f"❌ Error during scraping: {e}")

        # Try to save any results collected before the error
        if scraper.results:
            logger.info("💾 Attempting to save partial results...")
            try:
                scraper.save_to_json('C:/Users/MSI/OneDrive/Desktop/real_estate/Real_Estate_Data_Pipelines/raw_data/aqarmap_scraped_properties_detailed.json')
                logger.info("✅ Partial results saved")
            except:
                logger.warning("❌ Failed to save partial results")


if __name__ == "__main__":
    test_scrapers_operations()