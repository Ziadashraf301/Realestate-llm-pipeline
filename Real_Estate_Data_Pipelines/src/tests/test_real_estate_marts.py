import warnings
from src.config import config
from src.logger import LoggerFactory
from src.databases import Big_Query_Database
from src.etl import PropertyMartBuilder


def test_mart_builder_operations():
    """Execute mart-building pipeline with logging"""

    # Load config
    cfg = config

    # Initialize logger
    logger = LoggerFactory.create_logger(log_dir=cfg.LOG_DIR)

    logger.info("""
    ╔══════════════════════════════════════════════════════════╗
    ║        📊 Real Estate Property Mart Builder v1.0         ║
    ║   Cleans & transforms scraped data into analytics marts  ║
    ║   Includes: summary tables, quality checks, time series  ║
    ╚══════════════════════════════════════════════════════════╝
    """)

    warnings.filterwarnings("ignore")

    logger.info(f"✅ Loaded configuration")

    # Initialize database client
    database = Big_Query_Database(
        project_id=cfg.GCP_PROJECT_ID,
        raw_dataset_id=cfg.BQ_RAW_DATASET_ID,
        raw_table_id=cfg.BQ_RAW_TABLE_ID,
        mart_dataset_id=cfg.BQ_MART_DATASET_ID,
        mart_table_id=cfg.BQ_MART_TABLE_ID,
        log_dir=cfg.LOG_DIR,
    )

    database.connect()

    # Initialize mart builder
    mart_builder = PropertyMartBuilder(
        log_dir=cfg.LOG_DIR,
        db=database
    )

    logger.info("🚀 Starting mart building pipeline...\n")

    try:
        # Create Main Mart Table
        logger.info("🔧 Creating main mart table...")
        rows = mart_builder.create_mart_table()
        logger.info(f"✅ Main mart table created with {rows} rows\n")

        # Create Summary Tables
        logger.info("📊 Creating summary tables...")

        mart_builder.create_location_summary_mart()
        mart_builder.create_property_type_summary_mart()
        mart_builder.create_time_series_summary_mart()
        mart_builder.create_price_analysis_summary_mart()

        logger.info("✅ All summary tables created successfully\n")

        # Create Data Quality Report
        logger.info("🧪 Generating data quality report...")
        mart_builder.create_data_quality_report_mart()
        logger.info("✅ Data quality report generated successfully\n")

        # DONE
        logger.info("""
        🎉 MART BUILD COMPLETED SUCCESSFULLY!
        -------------------------------------------------------------
        Generated Tables:
          - Main Mart Table
          - Location Summary
          - Property Type Summary
          - Time Series Summary
          - Price Analysis Summary
          - Data Quality Report
        -------------------------------------------------------------
        """)

    except Exception as e:
        logger.error(f"❌ Mart build failed: {str(e)}")
        raise


if __name__ == "__main__":
    test_mart_builder_operations()
