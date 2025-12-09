"""
Real Estate Property Mart Builder
Transforms raw scraped property data into a clean, analytics-ready mart tables.
"""

from src.logger import LoggerFactory

class PropertyMartBuilder:
    """Creates optimized property mart tables from scraped data for analytics."""

    def __init__(self, log_dir, db):
        self.log_dir = log_dir
        self.db_client = None

        # Setup logging
        self.logger = LoggerFactory.create_logger(log_dir=self.log_dir)
        self.logger.info("Initialized PropertyMartBuilder")

        # Check the database connection
        if db.client is None:
            self.logger.error("Database connection failed")
            raise ConnectionError("Database connection failed")
        else:
            self.db_client = db

    def create_mart_table(self):
        self.logger.info("Starting: Create mart table")
        row_count = self.db_client.create_mart_table()
        self.logger.info(f"Mart table created successfully with {row_count} rows")
        return row_count

    def create_location_summary_mart(self):
        self.logger.info("Starting: Create location summary mart")
        self.db_client.create_location_summary()
        self.logger.info("Location summary mart created successfully")

    def create_property_type_summary_mart(self):
        self.logger.info("Starting: Create property type summary mart")
        self.db_client.create_property_type_summary()
        self.logger.info("Property type summary mart created successfully")

    def create_time_series_summary_mart(self):
        self.logger.info("Starting: Create time series summary mart")
        self.db_client.create_time_series_summary()
        self.logger.info("Time series summary mart created successfully")

    def create_price_analysis_summary_mart(self):
        self.logger.info("Starting: Create price analysis summary mart")
        self.db_client.create_price_analysis_summary()
        self.logger.info("Price analysis summary mart created successfully")

    def create_data_quality_report_mart(self):
        self.logger.info("Starting: Create data quality report mart")
        self.db_client.create_data_quality_report()
        self.logger.info("Data quality report mart created successfully")
