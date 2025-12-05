"""
Real Estate Property Mart Builder - OPTIMIZED & ENHANCED
--------------------------------------------------------
Transforms raw scraped property data into a clean, analytics-ready mart table.
Features: Arabic text normalization, data quality checks, partitioning, and comprehensive metrics.
"""

from google.cloud import bigquery
from pathlib import Path
import sys
import json
import os
import logging


class PropertyMartBuilder:
    """Creates optimized property mart tables from scraped data for analytics."""

    def __init__(self, project_id, dataset_id_raw='real_estate', table_id_raw='scraped_properties',
                 dataset_id_mart='real_estate_mart', table_id_mart='property_mart', 
                 log_file='C:/Users/MSI/OneDrive/Desktop/prtofolio/real_estate/aqarmap/logs/mart_builder.log'):
        self.project_id = project_id
        self.dataset_id_raw = dataset_id_raw
        self.table_id_raw = table_id_raw
        self.dataset_id_mart = dataset_id_mart
        self.table_id_mart = table_id_mart
        self.log_file = log_file

        self.bq_client = bigquery.Client(project=project_id)
        self.raw_table_ref = f"{project_id}.{dataset_id_raw}.{table_id_raw}"
        self.mart_table_ref = f"{project_id}.{dataset_id_mart}.{table_id_mart}"
        
        # Setup logging
        self._setup_logging()
        
        self._ensure_dataset_exists()

    def _setup_logging(self):
        """Configure logging to file and console"""
        log_dir = Path(self.log_file).parent
        log_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger = logging.getLogger('PropertyMartBuilder')
        self.logger.setLevel(logging.INFO)
        self.logger.handlers = []
        
        file_handler = logging.FileHandler(self.log_file, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        self.logger.info("="*70)
        self.logger.info("Property Mart Builder Initialized")
        self.logger.info(f"Log file: {self.log_file}")
        self.logger.info("="*70)

    def _ensure_dataset_exists(self):
        """Creates the mart dataset if it doesn't exist."""
        try:
            self.bq_client.get_dataset(f"{self.project_id}.{self.dataset_id_mart}")
            self.logger.info(f"✓ Dataset {self.dataset_id_mart} exists")
        except Exception:
            dataset = bigquery.Dataset(f"{self.project_id}.{self.dataset_id_mart}")
            dataset.location = "US"
            self.bq_client.create_dataset(dataset)
            self.logger.info(f"✅ Created dataset: {self.dataset_id_mart}")

    def create_mart_table(self):
        """Creates partitioned mart table with comprehensive data cleaning and enrichment."""
        self.logger.info("🚀 Starting optimized mart table creation...")

        query = f"""
        CREATE OR REPLACE TABLE `{self.mart_table_ref}`
        PARTITION BY scraped_date
        CLUSTER BY location, property_type, listing_type
        AS
        WITH cleaned_text AS (
            SELECT
                property_id,
                source,
                url,
                
                -- Simplified Arabic text cleaning using UDF-style approach
                TRIM(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    REGEXP_REPLACE(LOWER(COALESCE(title, '')), r'[إأآ]', 'ا'),
                                    r'[ىي]', 'ي'
                                ),
                                r'[ؤئ]', 'ء'
                            ),
                            r'[▪•●◼◾▫◽،/!؟💰:()+%.,-]', ' '
                        ),
                        r'\\s+', ' '
                    )
                ) AS title_cleaned,
                
                TRIM(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    REGEXP_REPLACE(LOWER(COALESCE(description, '')), r'[إأآ]', 'ا'),
                                    r'[ىي]', 'ي'
                                ),
                                r'[ؤئ]', 'ء'
                            ),
                            r'[▪•●◼◾▫◽،/!؟💰:()+%.,-]', ' '
                        ),
                        r'\\s+', ' '
                    )
                ) AS description_cleaned,
                
                TRIM(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            LOWER(COALESCE(address, '')),
                            r'[▪•●◼◾▫◽،/!؟💰:()+%.,-]', ' '
                        ),
                        r'\\s+', ' '
                    )
                ) AS address_cleaned,
                
                -- Numeric fields with validation
                CASE 
                    WHEN price_egp > 0 AND price_egp < 1000000000 THEN price_egp 
                    ELSE NULL 
                END AS price_egp,
                
                LOWER(TRIM(COALESCE(property_type, 'unknown'))) AS property_type,
                LOWER(TRIM(COALESCE(listing_type, 'unknown'))) AS listing_type,
                
                CASE 
                    WHEN SAFE_CAST(bedrooms AS INT64) BETWEEN 0 AND 20 
                    THEN SAFE_CAST(bedrooms AS INT64) 
                    ELSE NULL 
                END AS bedrooms,
                
                CASE 
                    WHEN SAFE_CAST(bathrooms AS INT64) BETWEEN 0 AND 15 
                    THEN SAFE_CAST(bathrooms AS INT64) 
                    ELSE NULL 
                END AS bathrooms,
                
                CASE 
                    WHEN SAFE_CAST(area_sqm AS FLOAT64) BETWEEN 10 AND 10000 
                    THEN SAFE_CAST(area_sqm AS FLOAT64) 
                    ELSE NULL 
                END AS area_sqm,
                
                CASE 
                    WHEN SAFE_CAST(floor_number AS INT64) BETWEEN -2 AND 100 
                    THEN SAFE_CAST(floor_number AS INT64) 
                    ELSE NULL 
                END AS floor_number,
                
                -- Geospatial validation
                CASE 
                    WHEN SAFE_CAST(latitude AS FLOAT64) BETWEEN 27.0 AND 33.0 
                    THEN SAFE_CAST(latitude AS FLOAT64) 
                    ELSE NULL 
                END AS latitude,
                
                CASE 
                    WHEN SAFE_CAST(longitude AS FLOAT64) BETWEEN 24.0 AND 34.0 
                    THEN SAFE_CAST(longitude AS FLOAT64) 
                    ELSE NULL 
                END AS longitude,
                
                TRIM(COALESCE(location, 'unknown')) AS location,
                TRIM(COALESCE(agent_type, 'unknown')) AS agent_type,
                
                -- Temporal fields
                EXTRACT(DATE FROM TIMESTAMP(scraped_at)) AS scraped_date,
                EXTRACT(YEAR FROM TIMESTAMP(scraped_at)) AS scraped_year,
                EXTRACT(MONTH FROM TIMESTAMP(scraped_at)) AS scraped_month,
                EXTRACT(DAY FROM TIMESTAMP(scraped_at)) AS scraped_day,
                EXTRACT(DAYOFWEEK FROM TIMESTAMP(scraped_at)) AS scraped_day_of_week,
                FORMAT_DATE('%B', EXTRACT(DATE FROM TIMESTAMP(scraped_at))) AS scraped_month_name
                
            FROM `{self.raw_table_ref}`
            WHERE scraped_at IS NOT NULL
        ),
        
        enriched AS (
            SELECT
                *,
                
                -- Price metrics
                SAFE_DIVIDE(price_egp, NULLIF(area_sqm, 0)) AS price_per_sqm,
                
                -- Size categorization
                CASE
                    WHEN area_sqm < 60 THEN 'compact'
                    WHEN area_sqm BETWEEN 60 AND 100 THEN 'small'
                    WHEN area_sqm BETWEEN 101 AND 150 THEN 'medium'
                    WHEN area_sqm BETWEEN 151 AND 250 THEN 'large'
                    WHEN area_sqm > 250 THEN 'xlarge'
                    ELSE 'unknown'
                END AS size_category,
                
                -- Bedroom categorization
                CASE
                    WHEN bedrooms = 0 THEN 'studio'
                    WHEN bedrooms = 1 THEN '1br'
                    WHEN bedrooms = 2 THEN '2br'
                    WHEN bedrooms = 3 THEN '3br'
                    WHEN bedrooms >= 4 THEN '4br+'
                    ELSE 'unknown'
                END AS bedroom_category,
                
                -- Price range categorization (in millions EGP)
                CASE
                    WHEN price_egp < 1000000 THEN 'under_1m'
                    WHEN price_egp BETWEEN 1000000 AND 2000000 THEN '1m_2m'
                    WHEN price_egp BETWEEN 2000001 AND 3000000 THEN '2m_3m'
                    WHEN price_egp BETWEEN 3000001 AND 5000000 THEN '3m_5m'
                    WHEN price_egp > 5000000 THEN 'over_5m'
                    ELSE 'unknown'
                END AS price_range,
                
                -- Data quality flags
                CASE 
                    WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN TRUE 
                    ELSE FALSE 
                END AS has_coordinates,
                
                CASE 
                    WHEN LENGTH(description_cleaned) > 20 THEN TRUE 
                    ELSE FALSE 
                END AS has_description,
                
                CASE
                    WHEN price_egp IS NOT NULL 
                        AND area_sqm IS NOT NULL 
                        AND latitude IS NOT NULL AND longitude IS NOT NULL
                    THEN 'complete'
                    ELSE 'incomplete'
                END AS data_quality
                
            FROM cleaned_text
        )
        
        SELECT
            -- Identity
            property_id,
            source,
            url,
            
            -- Text fields
            title_cleaned AS title,
            description_cleaned AS description,
            address_cleaned AS address,
            
            -- Categories
            property_type,
            listing_type,
            location,
            size_category,
            bedroom_category,
            price_range,
            
            -- Numeric metrics
            price_egp,
            price_per_sqm,
            bedrooms,
            bathrooms,
            area_sqm,
            floor_number,
            
            -- Agent info
            agent_type,
            
            -- Geospatial
            latitude,
            longitude,
            has_coordinates,
            
            -- Quality flags
            has_description,
            data_quality,
            
            -- Temporal
            scraped_date,
            scraped_year,
            scraped_month,
            scraped_month_name,
            scraped_day,
            scraped_day_of_week,
            
            -- Metadata
            CURRENT_TIMESTAMP() AS mart_updated_at
            
        FROM enriched;
        """

        try:
            job = self.bq_client.query(query)
            result = job.result()
            
            # Get row count
            row_count = self.bq_client.get_table(self.mart_table_ref).num_rows
            self.logger.info(f"✅ Mart table updated: {self.mart_table_ref}")
            self.logger.info(f"📊 Total rows: {row_count:,}")
            self.logger.info("="*70)
            return row_count
            
        except Exception as e:
            self.logger.error(f"❌ Error creating mart table: {str(e)}")
            raise

    def add_summary_tables(self):
        """Creates multiple summary tables for different analytical perspectives."""
        
        # 1. Location Summary
        self._create_location_summary()
        
        # 2. Property Type Summary
        self._create_property_type_summary()
        
        # 3. Time Series Summary
        self._create_time_series_summary()
        
        # 4. Price Analysis Summary
        self._create_price_analysis_summary()

    def _create_location_summary(self):
        """Location-based aggregations."""
        summary_ref = f"{self.project_id}.{self.dataset_id_mart}.location_summary"
        
        self.logger.info("📍 Building location summary...")
        query = f"""
        CREATE OR REPLACE TABLE `{summary_ref}` AS
        SELECT
            location,
            listing_type,
            COUNT(*) AS total_listings,
            COUNT(DISTINCT property_type) AS total_property_types,
            
            -- Price metrics
            ROUND(AVG(price_egp), 0) AS avg_price,
            ROUND(APPROX_QUANTILES(price_egp, 100)[OFFSET(50)], 0) AS median_price,
            ROUND(MIN(price_egp), 0) AS min_price,
            ROUND(MAX(price_egp), 0) AS max_price,
            
            -- Area metrics
            ROUND(AVG(area_sqm), 1) AS avg_area,
            ROUND(APPROX_QUANTILES(area_sqm, 100)[OFFSET(50)], 1) AS median_area,
            
            -- Price per sqm
            ROUND(AVG(price_per_sqm), 0) AS avg_price_per_sqm,
            ROUND(APPROX_QUANTILES(price_per_sqm, 100)[OFFSET(50)], 0) AS median_price_per_sqm,
            
            -- Room metrics
            ROUND(AVG(bedrooms), 1) AS avg_bedrooms,
            ROUND(AVG(bathrooms), 1) AS avg_bathrooms
            
        FROM `{self.mart_table_ref}`
        WHERE price_egp IS NOT NULL 
              AND price_egp > 1000
              AND area_sqm IS NOT NULL
        GROUP BY location, listing_type
        ORDER BY total_listings DESC;
        """
        
        try:
            job = self.bq_client.query(query)
            job.result()
            self.logger.info(f"✅ Location summary created: {summary_ref}")
        except Exception as e:
            self.logger.error(f"⚠️ Error creating location summary: {str(e)}")

    def _create_property_type_summary(self):
        """Property type aggregations."""
        summary_ref = f"{self.project_id}.{self.dataset_id_mart}.property_type_summary"
        
        self.logger.info("🏠 Building property type summary...")
        query = f"""
        CREATE OR REPLACE TABLE `{summary_ref}` AS
        SELECT
            property_type,
            listing_type,
            bedroom_category,
            COUNT(*) AS total_listings,
            
            -- Price statistics
            ROUND(AVG(price_egp), 0) AS avg_price,
            ROUND(STDDEV(price_egp), 0) AS stddev_price,
            ROUND(APPROX_QUANTILES(price_egp, 100)[OFFSET(25)], 0) AS price_p25,
            ROUND(APPROX_QUANTILES(price_egp, 100)[OFFSET(50)], 0) AS price_p50,
            ROUND(APPROX_QUANTILES(price_egp, 100)[OFFSET(75)], 0) AS price_p75,
            
            -- Area statistics
            ROUND(AVG(area_sqm), 1) AS avg_area,
            ROUND(MIN(area_sqm), 1) AS min_area,
            ROUND(MAX(area_sqm), 1) AS max_area,
            
            -- Price per sqm
            ROUND(AVG(price_per_sqm), 0) AS avg_price_per_sqm,
            
            
        FROM `{self.mart_table_ref}`
        WHERE price_egp IS NOT NULL 
              AND price_egp > 1000
              AND area_sqm IS NOT NULL
        GROUP BY property_type, listing_type, bedroom_category
        ORDER BY property_type, listing_type, bedroom_category;
        """
        
        try:
            job = self.bq_client.query(query)
            job.result()
            self.logger.info(f"✅ Property type summary created: {summary_ref}")
        except Exception as e:
            self.logger.error(f"⚠️ Error creating property type summary: {str(e)}")

    def _create_time_series_summary(self):
        """Time-based trends."""
        summary_ref = f"{self.project_id}.{self.dataset_id_mart}.time_series_summary"
        
        self.logger.info("📅 Building time series summary...")
        query = f"""
        CREATE OR REPLACE TABLE `{summary_ref}` AS
        SELECT
            scraped_date,
            scraped_year,
            scraped_month_name,
            listing_type,
            
            COUNT(*) AS total_listings,
            COUNT(DISTINCT location) AS unique_locations,
            
            ROUND(AVG(price_egp), 0) AS avg_price,
            ROUND(AVG(price_per_sqm), 0) AS avg_price_per_sqm,
            ROUND(AVG(area_sqm), 1) AS avg_area,
            
            -- Distribution by size
            COUNTIF(size_category = 'compact') AS compact_count,
            COUNTIF(size_category = 'small') AS small_count,
            COUNTIF(size_category = 'medium') AS medium_count,
            COUNTIF(size_category = 'large') AS large_count,
            COUNTIF(size_category = 'xlarge') AS xlarge_count
            
        FROM `{self.mart_table_ref}`
        WHERE price_egp IS NOT NULL 
              AND price_egp > 1000
              AND area_sqm IS NOT NULL
        GROUP BY scraped_date, scraped_year, scraped_month_name, listing_type
        ORDER BY scraped_date DESC, listing_type;
        """
        
        try:
            job = self.bq_client.query(query)
            job.result()
            self.logger.info(f"✅ Time series summary created: {summary_ref}")
        except Exception as e:
            self.logger.error(f"⚠️ Error creating time series summary: {str(e)}")

    def _create_price_analysis_summary(self):
        """Detailed price analysis."""
        summary_ref = f"{self.project_id}.{self.dataset_id_mart}.price_analysis_summary"
        
        self.logger.info("💰 Building price analysis summary...")
        query = f"""
        CREATE OR REPLACE TABLE `{summary_ref}` AS
        SELECT
            price_range,
            listing_type,
            property_type,
            location,
            
            COUNT(*) AS total_listings,
            
            -- Price metrics
            ROUND(AVG(price_egp), 0) AS avg_price,
            ROUND(MIN(price_egp), 0) AS min_price,
            ROUND(MAX(price_egp), 0) AS max_price,
            
            -- Area and efficiency
            ROUND(AVG(area_sqm), 1) AS avg_area,
            ROUND(AVG(price_per_sqm), 0) AS avg_price_per_sqm,
            
            -- Room configuration
            ROUND(AVG(bedrooms), 1) AS avg_bedrooms,
            ROUND(AVG(bathrooms), 1) AS avg_bathrooms,
            
        FROM `{self.mart_table_ref}`
        WHERE price_egp IS NOT NULL 
              AND price_egp > 1000
              AND area_sqm IS NOT NULL
        GROUP BY price_range, listing_type, property_type, location
        ORDER BY 
            CASE price_range
                WHEN 'under_1m' THEN 1
                WHEN '1m_2m' THEN 2
                WHEN '2m_3m' THEN 3
                WHEN '3m_5m' THEN 4
                WHEN 'over_5m' THEN 5
                ELSE 6
            END,
            listing_type,
            property_type;
        """
        
        try:
            job = self.bq_client.query(query)
            job.result()
            self.logger.info(f"✅ Price analysis summary created: {summary_ref}")
        except Exception as e:
            self.logger.error(f"⚠️ Error creating price analysis summary: {str(e)}")

    def create_data_quality_report(self):
        """Generates a data quality assessment report."""
        report_ref = f"{self.project_id}.{self.dataset_id_mart}.data_quality_report"
        
        self.logger.info("🔍 Building data quality report...")
        query = f"""
        CREATE OR REPLACE TABLE `{report_ref}` AS
        SELECT
            'Overall Statistics' AS metric_category,
            'Total Properties' AS metric_name,
            CAST(COUNT(*) AS STRING) AS metric_value
        FROM `{self.mart_table_ref}`
        UNION ALL
        
        SELECT
            'Data Completeness',
            'Complete Records (%)',
            CAST(ROUND(COUNTIF(data_quality = 'complete') * 100.0 / COUNT(*), 1) AS STRING)
        FROM `{self.mart_table_ref}`
        
        UNION ALL
        
        SELECT
            'Data Completeness',
            'Records with Coordinates (%)',
            CAST(ROUND(COUNTIF(has_coordinates) * 100.0 / COUNT(*), 1) AS STRING)
        FROM `{self.mart_table_ref}`
        
        UNION ALL
        
        SELECT
            'Data Completeness',
            'Records with Description (%)',
            CAST(ROUND(COUNTIF(has_description) * 100.0 / COUNT(*), 1) AS STRING)
        FROM `{self.mart_table_ref}`
        
        UNION ALL
        
        SELECT
            'Field Coverage',
            'Bedrooms Populated (%)',
            CAST(ROUND(COUNTIF(bedrooms IS NOT NULL) * 100.0 / COUNT(*), 1) AS STRING)
        FROM `{self.mart_table_ref}`
        
        UNION ALL
        
        SELECT
            'Field Coverage',
            'Bathrooms Populated (%)',
            CAST(ROUND(COUNTIF(bathrooms IS NOT NULL) * 100.0 / COUNT(*), 1) AS STRING)
        FROM `{self.mart_table_ref}`
        
        UNION ALL
        
        SELECT
            'Field Coverage',
            'Price Populated (%)',
            CAST(ROUND(COUNTIF(price_egp IS NOT NULL) * 100.0 / COUNT(*), 1) AS STRING)
        FROM `{self.mart_table_ref}`
        
        UNION ALL
        
        SELECT
            'Field Coverage',
            'Area Populated (%)',
            CAST(ROUND(COUNTIF(area_sqm IS NOT NULL) * 100.0 / COUNT(*), 1) AS STRING)
        FROM `{self.mart_table_ref}`
        
        UNION ALL
        
        SELECT
            'Data Distribution',
            'Unique Locations',
            CAST(COUNT(DISTINCT location) AS STRING)
        FROM `{self.mart_table_ref}`
        
        UNION ALL
        
        SELECT
            'Data Distribution',
            'Unique Property Types',
            CAST(COUNT(DISTINCT property_type) AS STRING)
        FROM `{self.mart_table_ref}`
        
        UNION ALL
        
        SELECT
            'Price Metrics',
            'Average Price (EGP)',
            CAST(ROUND(AVG(price_egp), 0) AS STRING)
        FROM `{self.mart_table_ref}`
        WHERE price_egp IS NOT NULL
        
        UNION ALL
        
        SELECT
            'Price Metrics',
            'Average Price per SQM (EGP)',
            CAST(ROUND(AVG(price_per_sqm), 0) AS STRING)
        FROM `{self.mart_table_ref}`
        WHERE price_per_sqm IS NOT NULL;
        """
        
        try:
            job = self.bq_client.query(query)
            job.result()
            self.logger.info(f"✅ Data quality report created: {report_ref}")
            
            # Print the report
            results = self.bq_client.query(f"SELECT * FROM `{report_ref}`").result()
            self.logger.info("\n" + "=" * 60)
            self.logger.info("DATA QUALITY REPORT")
            self.logger.info("=" * 60)
            for row in results:
                self.logger.info(f"{row.metric_category:20} | {row.metric_name:30} | {row.metric_value}")
            self.logger.info("=" * 60 + "\n")
            
        except Exception as e:
            self.logger.error(f"⚠️ Error creating data quality report: {str(e)}")


if __name__ == "__main__":
    # =============================================================================
    # Path Configuration
    # =============================================================================
    
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    sys.path.append(str(PROJECT_ROOT))

    CONFIG_DIR = PROJECT_ROOT / "config"
    TABLE_CONFIG_PATH = CONFIG_DIR / "table_config.json"
    CREDENTIALS_PATH = CONFIG_DIR / "big_query_service_account.json"
    
    print("\n" + "=" * 70)
    print("📂 PROPERTY MART BUILDER - CONFIGURATION")
    print("=" * 70)
    print(f"🔍 Project Root: {PROJECT_ROOT}")
    print(f"🔍 Config Directory: {CONFIG_DIR}")
    print(f"🔍 Table Config: {'✓' if TABLE_CONFIG_PATH.exists() else '✗'}")
    print(f"🔍 Credentials: {'✓' if CREDENTIALS_PATH.exists() else '✗'}")
    print("=" * 70 + "\n")

    # =============================================================================
    # Load Configuration
    # =============================================================================
    try:
        with open(TABLE_CONFIG_PATH, "r", encoding="utf-8") as f:
            table_config = json.load(f)
        print(f"✅ Loaded config from: {TABLE_CONFIG_PATH}\n")

        GCP_PROJECT_ID = table_config.get("GCP_PROJECT_ID")
        BQ_DATASET_ID = table_config.get("BQ_DATASET_ID")
        BQ_TABLE_ID = table_config.get("BQ_TABLE_ID")

    except FileNotFoundError:
        print(f"⚠️ WARNING: Missing table_config.json → using fallback defaults\n")
        GCP_PROJECT_ID = "your-gcp-project-id"
        BQ_DATASET_ID = "real_estate"
        BQ_TABLE_ID = "scraped_properties"
    
    # Set environment variables
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(CREDENTIALS_PATH)
    os.environ['GCP_PROJECT_ID'] = GCP_PROJECT_ID
    os.environ['BQ_DATASET_ID'] = BQ_DATASET_ID
    os.environ['BQ_TABLE_ID'] = BQ_TABLE_ID

    # =============================================================================
    # Execute Mart Building Pipeline
    # =============================================================================
    try:
        mart_builder = PropertyMartBuilder(GCP_PROJECT_ID)
        
        print("=" * 70)
        print("STEP 1: Creating Main Mart Table")
        print("=" * 70)
        mart_builder.create_mart_table()
        
        print("\n" + "=" * 70)
        print("STEP 2: Creating Summary Tables")
        print("=" * 70)
        mart_builder.add_summary_tables()
        
        print("\n" + "=" * 70)
        print("STEP 3: Generating Data Quality Report")
        print("=" * 70)
        mart_builder.create_data_quality_report()
        
        print("\n" + "=" * 70)
        print("🎉 MART BUILD COMPLETED SUCCESSFULLY!")
        print("=" * 70)
        print(f"Main Mart: {mart_builder.mart_table_ref}")
        print(f"Summary Tables:")
        print(f"  - location_summary")
        print(f"  - property_type_summary")
        print(f"  - time_series_summary")
        print(f"  - price_analysis_summary")
        print(f"  - data_quality_report")
        print("=" * 70 + "\n")
        
    except Exception as e:
        print(f"\n❌ ERROR: Mart build failed")
        print(f"Details: {str(e)}")
        raise