import warnings
from pathlib import Path
from src.scrapers import AQARMAPRealEstateScraper
from src.config import config
from src.databases import Big_Query_Database
from src.logger import LoggerFactory
from src.helpers import save_to_json, scraper_report


def test_scrapers_operations():
    """Execute the scraping pipeline with logging"""

    # Path Configuration
    PROJECT_ROOT = Path(__file__).resolve().parents[3]
    OUTPUT_JSON = PROJECT_ROOT / "Real_Estate_Data_Pipelines" / "raw_data" / "alexandria_for_sale.json"

    # Load Config
    cfg = config

    # Initialize Logger
    logger = LoggerFactory.create_logger(log_dir=cfg.LOG_DIR)

    logger.info("""
    ╔══════════════════════════════════════════════════════════╗
    ║   🏠 Egyptian Real Estate Scraper v2.0                   ║
    ║   Features: URL tracking, structured logs, BQ storage    ║
    ║   Target: Raw dataset ingestion                          ║
    ╚══════════════════════════════════════════════════════════╝
    """)

    warnings.filterwarnings("ignore")

    logger.info(f"✅ Loaded configuration")

    # Initialize BigQuery Database Client
    database = Big_Query_Database(
        project_id=cfg.GCP_PROJECT_ID,
        raw_dataset_id=cfg.BQ_RAW_DATASET_ID,
        raw_table_id=cfg.BQ_RAW_TABLE_ID,
        log_dir=cfg.LOG_DIR,
    )
    
    database.connect()

    # Initialize Scraper
    scraper = AQARMAPRealEstateScraper(
        log_dir=cfg.LOG_DIR,
        db=database
    )

    # Scraper Configuration
    CITY = "alexandria"
    LISTING_TYPE = "for-sale"
    MAX_PAGES = cfg.MAX_PAGES


    logger.info("⚙️  Scraper Configuration:")
    logger.info(f"  • City: {CITY}")
    logger.info(f"  • Listing Type: {LISTING_TYPE}")
    logger.info(f"  • Max Pages: {MAX_PAGES}")
    logger.info(f"  • Deep scraping: ENABLED")
    logger.info(f"  • URL tracking: ENABLED\n")

    # START SCRAPING
    logger.info("🚀 Starting AQARMAP scraping process...\n")

    try:
        scraper.scrape_aqarmap(
            city=CITY,
            listing_type=LISTING_TYPE,
            max_pages=MAX_PAGES
        )

        scraper_report(results=scraper.results, logger=logger)

        # SAVE RESULTS
        if scraper.results:
            logger.info("💾 Saving scraped properties...")

            # Save JSON
            save_to_json(filename=str(OUTPUT_JSON), results=scraper.results, logger=logger)
            logger.info(f"✅ Saved JSON → {OUTPUT_JSON}")

            # Save to BigQuery
            try:
                inserted_count = database.save_to_database(scraper.results)
                if inserted_count > 0:
                    logger.info(f"✅ Inserted {inserted_count} new properties into BigQuery")
                else:
                    logger.info("ℹ️  No new properties uploaded (all duplicates)")
            except Exception as upload_error:
                logger.warning(f"⚠️ BigQuery upload failed: {upload_error}")
                logger.info("   Data is still preserved in JSON")

            logger.info("🎉 Scraping completed successfully!")

        else:
            logger.warning("⚠️ No properties found to save")

    except KeyboardInterrupt:
        logger.warning("⛔ Scraping manually interrupted!")
        if scraper.results:
            save_to_json(filename=str(OUTPUT_JSON), results=scraper.results, logger=logger)
            logger.info("💾 Partial results saved before exit")

    except Exception as e:
        logger.error(f"❌ Unexpected scraper error: {e}")

        if scraper.results:
            try:
                fallback_path = OUTPUT_JSON.with_name("aqarmap_partial_fallback.json")
                save_to_json(filename=str(fallback_path), results=scraper.results, logger=logger)
                logger.info(f"💾 Partial results saved → {fallback_path}")
            except:
                logger.error("❌ Failed to save partial results")


if __name__ == "__main__":
    test_scrapers_operations()
