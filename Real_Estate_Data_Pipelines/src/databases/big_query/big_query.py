import os
from google.cloud import bigquery
import tempfile
import json as json_lib
from datetime import datetime
import time
from src.logger import LoggerFactory
from .schemes import PropertySchema
from ..db_models import PropertyModel

class Big_Query_Database():
    def __init__(self,
                log_dir,
                project_id, 
                raw_dataset_id=None, 
                raw_table_id=None,
                mart_dataset_id=None,
                mart_table_id=None):
        
        # BigQuery configuration
        self.project_id = project_id
        self.raw_dataset_id = raw_dataset_id
        self.raw_table_id = raw_table_id
        self.mart_dataset_id = mart_dataset_id
        self.mart_table_id = mart_table_id

        self.raw_table_ref = f"{project_id}.{raw_dataset_id}.{raw_table_id}"
        self.mart_table_ref = f"{project_id}.{mart_dataset_id}.{mart_table_id}"
        self.log_dir = log_dir
        
        # Initialize logger
        self.logger = LoggerFactory.create_logger(log_dir=self.log_dir)
        self.client = None

    def connect(self):
        # Initialize BigQuery client
        try:
            self.client = bigquery.Client(project=self.project_id)
            self.logger.info(f"✅ Connected to BigQuery project: {self.project_id}")
        except Exception as e:
            self.logger.info(f"❌ Failed to connect to BigQuery: {e}")
            raise


    def create_dataset_if_not_exists(self, project_id, dataset_id):
        """Creates the dataset if it doesn't exist."""
        try:
            self.client.get_dataset(f"{project_id}.{dataset_id}")
            self.logger.info(f"✅ Dataset {dataset_id} exists")
        except Exception:
            dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
            dataset.location = "US"
            self.client.create_dataset(dataset)
            self.logger.info(f"✅ Created dataset: {dataset_id}")


    def create_table_if_not_exists(self, table_ref, schema):
        try:
            self.client.get_table(table_ref)
            self.logger.info(f"✅ Table {table_ref} exists")
        except Exception as e:
            table = bigquery.Table(table_ref, schema=schema)
            table = self.client.create_table(table)
            self.logger.info(f"✅ Created table {table_ref}")


    def save_to_database(self, results):
        """Save results to BigQuery using batch load (free tier compatible)"""
        if not results:
            self.logger.warning("No data to save")
            return 0
        
        self.create_dataset_if_not_exists(project_id = self.project_id, dataset_id = self.raw_dataset_id)
        self.create_table_if_not_exists(table_ref = self.raw_table_ref, schema = PropertySchema)

        self.logger.info("📤 Uploading to BigQuery (Batch Mode)")

        # Prepare data for BigQuery
        new_items = []
        current_time = datetime.utcnow()
        
        for item in results:
            # Prepare item for BigQuery
            bq_item = {
                'property_id': item.get('property_id'),
                'source': item.get('source'),
                'url': item.get('url'),
                'title': item.get('title'),
                'description': item.get('description'),
                'price_egp': item.get('price_egp'),
                'price_text': item.get('price_text'),
                'currency': item.get('currency'),
                'property_type': item.get('property_type'),
                'listing_type': item.get('listing_type'),
                
                # Property details
                'bedrooms': item.get('bedrooms'),
                'bathrooms': item.get('bathrooms'),
                'area_sqm': item.get('area_sqm'),
                'floor_number': item.get('floor_number'),
                
                # Location details
                'location': item.get('location'),
                'address': item.get('address'),
                'latitude': item.get('latitude'),
                'longitude': item.get('longitude'),
                'last_updated': item.get('last_updated'),
                
                # Images (convert list to JSON string)
                'images': json_lib.dumps(item.get('images', [])),
                'image_count': len(item.get('images', [])),
                
                # Agent information
                'agent_type': item.get('agent_type'),
                
                # Timestamps
                'scraped_at': item.get('scraped_at'),
                'loaded_at': current_time.isoformat(),
            }
            
            try:
                validated_item = PropertyModel(**bq_item)
                new_items.append(validated_item.dict())
            except Exception as e:
                self.logger.error(f"❌ Invalid row skipped: {e}")
        
        # Use batch load with temporary JSON file (FREE TIER COMPATIBLE)
        self.logger.info(f"📤 Batch loading {len(new_items)} new properties...")
        try:
            # Create temporary NDJSON file (newline-delimited JSON)
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as temp_file:
                for item in new_items:
                    json_lib.dump(item, temp_file)
                    temp_file.write('\n')
                temp_file_path = temp_file.name
            
            # Configure load job
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                schema=PropertySchema,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                autodetect=False
            )
            
            # Load data from file
            with open(temp_file_path, 'rb') as source_file:
                load_job = self.client.load_table_from_file(
                    source_file,
                    self.raw_table_ref,
                    job_config=job_config
                )
            
            # Wait for job to complete
            self.logger.info("⏳ Waiting for load job to complete...")
            load_job.result()
            
            # Clean up temp file
            os.unlink(temp_file_path)
            
            self.logger.info("✅ BigQuery Upload Summary:")
            self.logger.info(f"🆕 New properties inserted: {len(new_items)}")
            self.logger.info(f"🗂️ Table: {self.raw_table_ref}")
            return len(new_items)
            
        except Exception as e:
            self.logger.error(f"❌ Failed to load data: {e}")
            # Clean up temp file
            try:
                if 'temp_file_path' in locals():
                    os.unlink(temp_file_path)
            except:
                pass
            return 0

    def load_existing_urls_from_database(self):
        """Load existing property URLs from BigQuery"""
        try:
            query = f"""
                SELECT DISTINCT url 
                FROM `{self.raw_table_ref}`
            """
            self.logger.info("🔍 Loading existing URLs from BigQuery...")
            query_job = self.client.query(query)
            existing_urls = {row.url for row in query_job.result()}
            self.logger.info(f"📂 Loaded {len(existing_urls)} existing URLs from BigQuery")
            return existing_urls
        
        except Exception as e:
            self.logger.info(f"⚠️  Could not load existing URLs (table may not exist yet): {e}")
            return set()


    
    def get_validated_properties_for_vectordb(self, limit=None):
        """
        Pull ONLY validated, high-quality properties from BigQuery mart for Vector Database.
        
        Args:
            limit: Maximum number of rows (None for all)
            
        Returns:
            List of validated property dicts ready for vectorization
        """
        if not self.client:
            raise RuntimeError("Not connected to BigQuery")
        
        self.logger.info("🔍 Fetching validated properties from BigQuery mart...")
        
        limit_clause = f"LIMIT {limit}" if limit else ""
        
        query = f"""
        SELECT
            property_id,
            source,
            url,
            title,
            description,
            address,
            property_type,
            listing_type,
            location,
            price_egp,
            bedrooms,
            bathrooms,
            area_sqm,
            floor_number,
            latitude,
            longitude
            
        FROM `{self.mart_table_ref}`
        WHERE 
            -- Quality filters
            data_quality = 'complete'
            AND has_description = true
            AND price_egp > 1000
            AND area_sqm >= 10
            AND bedrooms BETWEEN 0 AND 25
            AND bathrooms BETWEEN 0 AND 15
            AND LENGTH(title) >= 3
            AND LENGTH(description) >= 10
        
        {limit_clause}
        """
        
        try:
            query_job = self.client.query(query)
            results = query_job.result()
            
            properties = [dict(row.items()) for row in results]
            
            self.logger.info(f"✅ Retrieved {len(properties):,} validated properties")
            return properties
            
        except Exception as e:
            self.logger.error(f"❌ Failed to fetch properties: {e}")
            raise
    

    def create_mart_table(self):
        """Creates partitioned mart table with comprehensive data cleaning and enrichment."""
        self.logger.info("🚀 Starting mart table creation...")
        self.create_dataset_if_not_exists(project_id = self.project_id, dataset_id = self.mart_dataset_id)

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
                    ELSE 0 
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
            self.client.query(query)
            
            # Get row count
            time.sleep(3)
            row_count = self.client.get_table(self.mart_table_ref).num_rows
            self.logger.info(f"✅ Mart table updated: {self.mart_table_ref}")
            self.logger.info(f"📊 Total rows: {row_count:,}")
            return row_count
            
        except Exception as e:
            self.logger.error(f"❌ Error creating mart table: {str(e)}")
            raise

    def create_location_summary(self):
        """Location-based aggregations."""
        summary_ref = f"{self.project_id}.{self.mart_dataset_id}.location_summary"
        
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
            self.client.query(query)
            self.logger.info(f"✅ Location summary created: {summary_ref}")
        except Exception as e:
            self.logger.error(f"⚠️ Error creating location summary: {str(e)}")

    def create_property_type_summary(self):
        """Property type aggregations."""
        summary_ref = f"{self.project_id}.{self.mart_dataset_id}.property_type_summary"
        
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
            self.client.query(query)
            self.logger.info(f"✅ Property type summary created: {summary_ref}")
        except Exception as e:
            self.logger.error(f"⚠️ Error creating property type summary: {str(e)}")

    def create_time_series_summary(self):
        """Time-based trends."""
        summary_ref = f"{self.project_id}.{self.mart_dataset_id}.time_series_summary"
        
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
            job = self.client.query(query)
            job.result()
            self.logger.info(f"✅ Time series summary created: {summary_ref}")
        except Exception as e:
            self.logger.error(f"⚠️ Error creating time series summary: {str(e)}")

    def create_price_analysis_summary(self):
        """Detailed price analysis."""
        summary_ref = f"{self.project_id}.{self.mart_dataset_id}.price_analysis_summary"
        
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
            job = self.client.query(query)
            job.result()
            self.logger.info(f"✅ Price analysis summary created: {summary_ref}")
        except Exception as e:
            self.logger.error(f"⚠️ Error creating price analysis summary: {str(e)}")

    def create_data_quality_report(self):
        """Generates a data quality assessment report."""
        report_ref = f"{self.project_id}.{self.mart_dataset_id}.data_quality_report"
        
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
            job = self.client.query(query)
            job.result()
            self.logger.info(f"✅ Data quality report created: {report_ref}")
            
            # Print the report
            results = self.client.query(f"SELECT * FROM `{report_ref}`").result()
            self.logger.info("\n" + "=" * 60)
            self.logger.info("DATA QUALITY REPORT")
            self.logger.info("=" * 60)
            for row in results:
                self.logger.info(f"{row.metric_category:20} | {row.metric_name:30} | {row.metric_value}")
            self.logger.info("=" * 60 + "\n")
            
        except Exception as e:
            self.logger.error(f"⚠️ Error creating data quality report: {str(e)}")


            