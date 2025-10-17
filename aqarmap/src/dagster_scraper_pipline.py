"""
Dagster Job for Complete Real Estate Pipeline
----------------------------------------------
1. Scrape properties from AQARMAP
2. Load to BigQuery raw table
3. Transform to Mart table (incremental)
4. Update all summary tables
5. Process to Milvus vector database
"""

import os
from pathlib import Path
import json
from datetime import datetime
import sys

# ============================================================================
# CONFIGURATION
# ============================================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

CONFIG_DIR = PROJECT_ROOT / 'config'

GOOGLE_APPLICATION_CREDENTIALS = os.path.join(CONFIG_DIR, 'big_query_service_account.json')
TABLE_CONFIG_PATH = os.path.join(CONFIG_DIR, 'table_config.json')

print("\n" + "="*70)
print("📂 Loading Configuration Files")
print("="*70)

try:
    with open(TABLE_CONFIG_PATH, 'r') as f:
        table_config = json.load(f)
    print(f"✅ Table config loaded from: {TABLE_CONFIG_PATH}")
    
    GCP_PROJECT_ID = table_config.get('GCP_PROJECT_ID')
    BQ_DATASET_ID = table_config.get('BQ_DATASET_ID')
    BQ_TABLE_ID = table_config.get('BQ_TABLE_ID')
    MAX_PAGES = table_config.get('MAX_PAGES', 1)
    LOG_FILE = table_config.get('LOG_FILE', 'aqarmap/logs/scraper.log')
    
    # Milvus configuration
    MILVUS_HOST = table_config.get('MILVUS_HOST', 'localhost')
    MILVUS_PORT = table_config.get('MILVUS_PORT', '19530')
    EMBEDDING_MODEL = table_config.get('EMBEDDING_MODEL', 'all-MiniLM-L6-v2')
    VECTOR_BATCH_SIZE = table_config.get('VECTOR_BATCH_SIZE', 100)
    
    # Mart configuration
    BQ_MART_DATASET_ID = table_config.get('BQ_MART_DATASET_ID', 'real_estate_mart')
    BQ_MART_TABLE_ID = table_config.get('BQ_MART_TABLE_ID', 'property_mart')
    MART_LOG_FILE = table_config.get('MART_LOG_FILE', 'logs/mart_builder.log')
    
except FileNotFoundError:
    print(f"⚠️ WARNING: table_config.json not found at: {TABLE_CONFIG_PATH}")
    GCP_PROJECT_ID = 'your-gcp-project-id'
    BQ_DATASET_ID = 'real_estate'
    BQ_TABLE_ID = 'scraped_properties'
    MAX_PAGES = 1
    LOG_FILE = 'logs/scraper.log'
    MILVUS_HOST = 'localhost'
    MILVUS_PORT = '19530'
    EMBEDDING_MODEL = 'all-MiniLM-L6-v2'
    VECTOR_BATCH_SIZE = 100
    BQ_MART_DATASET_ID = 'real_estate_mart'
    BQ_MART_TABLE_ID = 'property_mart'
    MART_LOG_FILE = 'logs/mart_builder.log'

# Validate
if not all([GCP_PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID]):
    raise ValueError("❌ Missing required configuration values in table_config.json")

if GCP_PROJECT_ID == 'your-gcp-project-id':
    raise ValueError("❌ Please update GCP_PROJECT_ID in table_config.json")

# Create directories
os.makedirs('C:/Users/MSI/OneDrive/Desktop/prtofolio/real_estate/aqarmap/logs', exist_ok=True)
os.makedirs('C:/Users/MSI/OneDrive/Desktop/prtofolio/real_estate/aqarmap/raw_data', exist_ok=True)
print("✅ Created/verified logs and raw_data directories")

# Set environment variables
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GOOGLE_APPLICATION_CREDENTIALS
os.environ['GCP_PROJECT_ID'] = GCP_PROJECT_ID
os.environ['BQ_DATASET_ID'] = BQ_DATASET_ID
os.environ['BQ_TABLE_ID'] = BQ_TABLE_ID

print("\n" + "="*70)
print("🔧 Dagster Complete Pipeline Configuration")
print("="*70)
print(f"📊 GCP Project ID: {GCP_PROJECT_ID}")
print(f"📊 BigQuery Raw Dataset: {BQ_DATASET_ID}")
print(f"📊 BigQuery Raw Table: {BQ_TABLE_ID}")
print(f"📊 BigQuery Mart Dataset: {BQ_MART_DATASET_ID}")
print(f"📊 BigQuery Mart Table: {BQ_MART_TABLE_ID}")
print(f"📄 Max Pages per scrape: {MAX_PAGES}")
print(f"📝 Mart Log File: {MART_LOG_FILE}")
print(f"🔗 Milvus Host: {MILVUS_HOST}:{MILVUS_PORT}")
print("="*70 + "\n")

# ============================================================================
# Import Dagster and custom modules
# ============================================================================
from dagster import (
    asset,
    define_asset_job,
    ScheduleDefinition,
    Definitions,
    OpExecutionContext,
    ConfigurableResource,
    RetryPolicy,
)
from pydantic import Field
from src.real_estate_scraper import AQARMAPRealEstateScraper
from src.real_estate_vector_processor import RealEstateMilvusProcessor
from src.real_estate_mart import PropertyMartBuilder


class ScraperConfig(ConfigurableResource):
    """Configuration for the scraper"""
    project_id: str = Field(description="GCP Project ID")
    dataset_id: str = Field(description="BigQuery Dataset ID")
    table_id: str = Field(description="BigQuery Table ID")
    log_file: str = Field(description="Log file path")
    max_pages: int = Field(description="Maximum pages to scrape per city")


class MartConfig(ConfigurableResource):
    """Configuration for mart builder"""
    project_id: str = Field(description="GCP Project ID")
    dataset_id_raw: str = Field(description="BigQuery Raw Dataset ID")
    table_id_raw: str = Field(description="BigQuery Raw Table ID")
    dataset_id_mart: str = Field(description="BigQuery Mart Dataset ID")
    table_id_mart: str = Field(description="BigQuery Mart Table ID")
    log_file: str = Field(description="Log file path for mart builder")


class VectorProcessorConfig(ConfigurableResource):
    """Configuration for vector processor"""
    project_id: str = Field(description="GCP Project ID")
    dataset_id: str = Field(description="BigQuery Dataset ID (Mart)")
    table_id: str = Field(description="BigQuery Table ID (Mart)")
    milvus_host: str = Field(description="Milvus server host")
    milvus_port: str = Field(description="Milvus server port")
    embedding_model: str = Field(description="Embedding model name")
    batch_size: int = Field(description="Batch size for processing")


# ============================================================================
# SCRAPING ASSETS (unchanged)
# ============================================================================

@asset(
    description="Scrape Alexandria properties for sale",
    group_name="real_estate_scraping",
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def scrape_alexandria_for_sale(context: OpExecutionContext, scraper_config: ScraperConfig):
    """Scrape Alexandria properties for sale"""
    try:
        context.log.info("🏠 Starting Alexandria for-sale scraping...")
        
        scraper = AQARMAPRealEstateScraper(
            project_id=scraper_config.project_id,
            dataset_id=scraper_config.dataset_id,
            table_id=scraper_config.table_id,
            log_file=scraper_config.log_file
        )
        
        results = scraper.scrape_aqarmap(
            city='alexandria',
            listing_type='for-sale',
            max_pages=scraper_config.max_pages
        )
        
        context.log.info(f"✅ Scraped {len(results)} properties from Alexandria (for-sale)")
        
        inserted_count = scraper.save_to_bigquery()
        context.log.info(f"📤 Inserted {inserted_count} new properties to BigQuery")
        
        scraper.save_to_json('C:/Users/MSI/OneDrive/Desktop/prtofolio/real_estate/aqarmap/raw_data/alexandria_for_sale.json')
        
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
def scrape_alexandria_for_rent(context: OpExecutionContext, scraper_config: ScraperConfig):
    """Scrape Alexandria properties for rent"""
    try:
        context.log.info("🏠 Starting Alexandria for-rent scraping...")
        
        scraper = AQARMAPRealEstateScraper(
            project_id=scraper_config.project_id,
            dataset_id=scraper_config.dataset_id,
            table_id=scraper_config.table_id,
            log_file=scraper_config.log_file
        )
        
        results = scraper.scrape_aqarmap(
            city='alexandria',
            listing_type='for-rent',
            max_pages=scraper_config.max_pages
        )
        
        context.log.info(f"✅ Scraped {len(results)} properties from Alexandria (for-rent)")
        
        inserted_count = scraper.save_to_bigquery()
        context.log.info(f"📤 Inserted {inserted_count} new properties to BigQuery")
        
        scraper.save_to_json('C:/Users/MSI/OneDrive/Desktop/prtofolio/real_estate/aqarmap/raw_data/alexandria_for_rent.json')
        
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
def scrape_cairo_for_sale(context: OpExecutionContext, scraper_config: ScraperConfig):
    """Scrape Cairo properties for sale"""
    try:
        context.log.info("🏠 Starting Cairo for-sale scraping...")
        
        scraper = AQARMAPRealEstateScraper(
            project_id=scraper_config.project_id,
            dataset_id=scraper_config.dataset_id,
            table_id=scraper_config.table_id,
            log_file=scraper_config.log_file
        )
        
        results = scraper.scrape_aqarmap(
            city='cairo',
            listing_type='for-sale',
            max_pages=scraper_config.max_pages
        )
        
        context.log.info(f"✅ Scraped {len(results)} properties from Cairo (for-sale)")
        
        inserted_count = scraper.save_to_bigquery()
        context.log.info(f"📤 Inserted {inserted_count} new properties to BigQuery")
        
        scraper.save_to_json('C:/Users/MSI/OneDrive/Desktop/prtofolio/real_estate/aqarmap/raw_data/cairo_for_sale.json')
        
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
def scrape_cairo_for_rent(context: OpExecutionContext, scraper_config: ScraperConfig):
    """Scrape Cairo properties for rent"""
    try:
        context.log.info("🏠 Starting Cairo for-rent scraping...")
        
        scraper = AQARMAPRealEstateScraper(
            project_id=scraper_config.project_id,
            dataset_id=scraper_config.dataset_id,
            table_id=scraper_config.table_id,
            log_file=scraper_config.log_file
        )
        
        results = scraper.scrape_aqarmap(
            city='cairo',
            listing_type='for-rent',
            max_pages=scraper_config.max_pages
        )
        
        context.log.info(f"✅ Scraped {len(results)} properties from Cairo (for-rent)")
        
        inserted_count = scraper.save_to_bigquery()
        context.log.info(f"📤 Inserted {inserted_count} new properties to BigQuery")
        
        scraper.save_to_json('C:/Users/MSI/OneDrive/Desktop/prtofolio/real_estate/aqarmap/raw_data/cairo_for_rent.json')
        
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


@asset(
    description="Summary of all scraping operations",
    group_name="real_estate_scraping",
    deps=[
        "scrape_alexandria_for_sale",
        "scrape_alexandria_for_rent",
        "scrape_cairo_for_sale",
        "scrape_cairo_for_rent"
    ]
)
def scraping_summary(
    context: OpExecutionContext,
    scrape_alexandria_for_sale,
    scrape_alexandria_for_rent,
    scrape_cairo_for_sale,
    scrape_cairo_for_rent
):
    """Generate summary of all scraping operations"""
    
    all_results = [
        scrape_alexandria_for_sale,
        scrape_alexandria_for_rent,
        scrape_cairo_for_sale,
        scrape_cairo_for_rent
    ]
    
    total_scraped = sum(r.get('scraped_count', 0) for r in all_results)
    total_inserted = sum(r.get('inserted_count', 0) for r in all_results)
    successful = sum(1 for r in all_results if r.get('status') == 'success')
    failed = sum(1 for r in all_results if r.get('status') == 'failed')
    
    summary = {
        "timestamp": datetime.now().isoformat(),
        "total_properties_scraped": total_scraped,
        "total_properties_inserted": total_inserted,
        "successful_operations": successful,
        "failed_operations": failed,
        "details": all_results
    }
    
    context.log.info("="*60)
    context.log.info("📊 SCRAPING SUMMARY")
    context.log.info("="*60)
    context.log.info(f"✅ Successful operations: {successful}/4")
    context.log.info(f"❌ Failed operations: {failed}/4")
    context.log.info(f"📈 Total properties scraped: {total_scraped}")
    context.log.info(f"💾 Total properties inserted to BigQuery: {total_inserted}")
    context.log.info("="*60)
    
    try:
        with open('C:/Users/MSI/OneDrive/Desktop/prtofolio/real_estate/aqarmap/raw_data/scraping_summary.json', 'w') as f:
            json.dump(summary, f, indent=2)
        context.log.info("✅ Summary saved to raw_data/scraping_summary.json")
    except Exception as e:
        context.log.error(f"⚠️ Could not save summary: {e}")
    
    return summary


# ============================================================================
# MART ETL ASSETS
# ============================================================================

@asset(
    description="Transform raw data to property mart table",
    group_name="mart_transformation",
    deps=["scraping_summary"],
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def property_mart(
    context: OpExecutionContext,
    mart_config: MartConfig,
    scraping_summary
):
    """Transform raw scraped data into cleaned mart table"""
    try:
        context.log.info("🔄 Starting mart table transformation...")
        context.log.info(f"   New properties from scraping: {scraping_summary.get('total_properties_inserted', 0)}")
        
        # Initialize mart builder with logging
        mart_builder = PropertyMartBuilder(
            project_id=mart_config.project_id,
            dataset_id_raw=mart_config.dataset_id_raw,
            table_id_raw=mart_config.table_id_raw,
            dataset_id_mart=mart_config.dataset_id_mart,
            table_id_mart=mart_config.table_id_mart,
            log_file=mart_config.log_file
        )
        
        # Run Create Mart table
        total_rows = mart_builder.create_mart_table()
        
        context.log.info(f"✅ Mart table created with total {total_rows:,} rows")
        
        return {
            "total_rows": total_rows,
            "new_from_scraping": scraping_summary.get('total_properties_inserted', 0),
            "timestamp": datetime.now().isoformat(),
            "status": "success"
        }
        
    except Exception as e:
        context.log.error(f"❌ Error transforming to mart: {str(e)}")
        import traceback
        context.log.error(traceback.format_exc())
        return {
            "total_rows": 0,
            "new_from_scraping": 0,
            "timestamp": datetime.now().isoformat(),
            "status": "failed",
            "error": str(e)
        }


@asset(
    description="Location-based summary aggregations",
    group_name="mart_summaries",
    deps=["property_mart"],
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def location_summary(
    context: OpExecutionContext,
    mart_config: MartConfig
):
    """Location summary snapshot"""
    try:
        context.log.info("🗺️ Creating location summary snapshot...")
        
        mart_builder = PropertyMartBuilder(
            project_id=mart_config.project_id,
            dataset_id_raw=mart_config.dataset_id_raw,
            table_id_raw=mart_config.table_id_raw,
            dataset_id_mart=mart_config.dataset_id_mart,
            table_id_mart=mart_config.table_id_mart,
            log_file=mart_config.log_file
        )
        
        mart_builder._create_location_summary()
        
        context.log.info("✅ Location summary snapshot created successfully")
        
        return {
            "snapshot_date": datetime.now().date().isoformat(),
            "timestamp": datetime.now().isoformat(),
            "status": "success"
        }
        
    except Exception as e:
        context.log.error(f"❌ Error creating location summary: {str(e)}")
        return {
            "snapshot_date": datetime.now().date().isoformat(),
            "timestamp": datetime.now().isoformat(),
            "status": "failed",
            "error": str(e)
        }


@asset(
    description="Property type summary aggregations",
    group_name="mart_summaries",
    deps=["property_mart"],
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def property_type_summary(
    context: OpExecutionContext,
    mart_config: MartConfig
):
    """Creating new property type summary snapshot"""
    try:
        context.log.info("🏠 Creating property type summary snapshot...")
        
        mart_builder = PropertyMartBuilder(
            project_id=mart_config.project_id,
            dataset_id_raw=mart_config.dataset_id_raw,
            table_id_raw=mart_config.table_id_raw,
            dataset_id_mart=mart_config.dataset_id_mart,
            table_id_mart=mart_config.table_id_mart,
            log_file=mart_config.log_file
        )
        
        mart_builder._create_property_type_summary()
        
        context.log.info("✅ Property type summary snapshot created successfully")
        
        return {
            "snapshot_date": datetime.now().date().isoformat(),
            "timestamp": datetime.now().isoformat(),
            "status": "success"
        }
        
    except Exception as e:
        context.log.error(f"❌ Error creating property type summary: {str(e)}")
        return {
            "snapshot_date": datetime.now().date().isoformat(),
            "timestamp": datetime.now().isoformat(),
            "status": "failed",
            "error": str(e)
        }


@asset(
    description="Time series summary aggregations",
    group_name="mart_summaries",
    deps=["property_mart"],
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def time_series_summary(
    context: OpExecutionContext,
    mart_config: MartConfig
):
    """Create new time series summary snapshot"""
    try:
        context.log.info("📅 Creating time series summary snapshot...")
        
        mart_builder = PropertyMartBuilder(
            project_id=mart_config.project_id,
            dataset_id_raw=mart_config.dataset_id_raw,
            table_id_raw=mart_config.table_id_raw,
            dataset_id_mart=mart_config.dataset_id_mart,
            table_id_mart=mart_config.table_id_mart,
            log_file=mart_config.log_file
        )
        
        mart_builder._create_time_series_summary()
        
        context.log.info("✅ Time series summary snapshot created successfully")
        
        return {
            "snapshot_date": datetime.now().date().isoformat(),
            "timestamp": datetime.now().isoformat(),
            "status": "success"
        }
        
    except Exception as e:
        context.log.error(f"❌ Error creating time series summary: {str(e)}")
        return {
            "snapshot_date": datetime.now().date().isoformat(),
            "timestamp": datetime.now().isoformat(),
            "status": "failed",
            "error": str(e)
        }


@asset(
    description="Price analysis summary aggregations",
    group_name="mart_summaries",
    deps=["property_mart"],
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def price_analysis_summary(
    context: OpExecutionContext,
    mart_config: MartConfig
):
    """Create new price analysis summary snapshot"""
    try:
        context.log.info("💰 Creating price analysis summary snapshot...")
        
        mart_builder = PropertyMartBuilder(
            project_id=mart_config.project_id,
            dataset_id_raw=mart_config.dataset_id_raw,
            table_id_raw=mart_config.table_id_raw,
            dataset_id_mart=mart_config.dataset_id_mart,
            table_id_mart=mart_config.table_id_mart,
            log_file=mart_config.log_file
        )
        
        mart_builder._create_price_analysis_summary()
        
        context.log.info("✅ Price analysis summary snapshot created successfully")
        
        return {
            "snapshot_date": datetime.now().date().isoformat(),
            "timestamp": datetime.now().isoformat(),
            "status": "success"
        }
        
    except Exception as e:
        context.log.error(f("❌ Error creating price analysis summary: {str(e)}"))
        return {
            "snapshot_date": datetime.now().date().isoformat(),
            "timestamp": datetime.now().isoformat(),
            "status": "failed",
            "error": str(e)
        }


@asset(
    description="Data quality report",
    group_name="mart_summaries",
    deps=["property_mart"],
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def data_quality_report(
    context: OpExecutionContext,
    mart_config: MartConfig
):
    """Create new data quality report snapshot"""
    try:
        context.log.info("🔍 Creating data quality report snapshot...")
        
        mart_builder = PropertyMartBuilder(
            project_id=mart_config.project_id,
            dataset_id_raw=mart_config.dataset_id_raw,
            table_id_raw=mart_config.table_id_raw,
            dataset_id_mart=mart_config.dataset_id_mart,
            table_id_mart=mart_config.table_id_mart,
            log_file=mart_config.log_file
        )
        
        mart_builder.create_data_quality_report()
        
        context.log.info("✅ Data quality report snapshot created successfully")
        
        return {
            "snapshot_date": datetime.now().date().isoformat(),
            "timestamp": datetime.now().isoformat(),
            "status": "success"
        }
        
    except Exception as e:
        context.log.error(f"❌ Error creating data quality report: {str(e)}")
        return {
            "snapshot_date": datetime.now().date().isoformat(),
            "timestamp": datetime.now().isoformat(),
            "status": "failed",
            "error": str(e)
        }


@asset(
    description="Summary of all mart transformations",
    group_name="mart_summaries",
    deps=[
        "location_summary",
        "property_type_summary",
        "time_series_summary",
        "price_analysis_summary",
        "data_quality_report"
    ]
)
def mart_transformation_summary(
    context: OpExecutionContext,
    property_mart,
    location_summary,
    property_type_summary,
    time_series_summary,
    price_analysis_summary,
    data_quality_report
):
    """Generate summary of all mart transformation operations"""
    
    all_results = [
        location_summary,
        property_type_summary,
        time_series_summary,
        price_analysis_summary,
        data_quality_report
    ]
    
    successful = sum(1 for r in all_results if r.get('status') == 'success')
    failed = sum(1 for r in all_results if r.get('status') == 'failed')
    
    summary = {
        "timestamp": datetime.now().isoformat(),
        "mart_total_rows": property_mart.get('total_rows', 0),
        "mart_status": property_mart.get('status', 'unknown'),
        "summary_tables_successful": successful,
        "summary_tables_failed": failed,
        "details": {
            "mart": property_mart,
            "location_summary": location_summary,
            "property_type_summary": property_type_summary,
            "time_series_summary": time_series_summary,
            "price_analysis_summary": price_analysis_summary,
            "data_quality_report": data_quality_report
        }
    }
    
    context.log.info("="*60)
    context.log.info("📊 MART TRANSFORMATION SUMMARY")
    context.log.info("="*60)
    context.log.info(f"✅ Mart table: {property_mart.get('total_rows', 0):,} total rows")
    context.log.info(f"✅ Successful summary tables: {successful}/5")
    context.log.info(f"❌ Failed summary tables: {failed}/5")
    context.log.info("="*60)
    
    try:
        with open('C:/Users/MSI/OneDrive/Desktop/prtofolio/real_estate/aqarmap/raw_data/mart_summary.json', 'w') as f:
            json.dump(summary, f, indent=2)
        context.log.info("✅ Mart summary saved")
    except Exception as e:
        context.log.error(f"⚠️ Could not save mart summary: {e}")
    
    return summary


# ============================================================================
# VECTOR PROCESSING ASSETS
# ============================================================================

@asset(
    description="Process raw data to Milvus vector database",
    group_name="vector_processing",
    deps=["scraping_summary"],
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def process_to_milvus(
    context: OpExecutionContext,
    vector_config: VectorProcessorConfig,
    scraping_summary
):
    """Process properties from Raw table and store in Milvus"""
    try:
        context.log.info("🤖 Starting vector processing from mart...")
        context.log.info(f"   Data rows: {scraping_summary.get('total_rows', 0)}")
        
        # Initialize processor (now reading from mart table)
        processor = RealEstateMilvusProcessor(
            project_id=vector_config.project_id,
            dataset_id=vector_config.dataset_id,  
            table_id=vector_config.table_id,      
            milvus_host=vector_config.milvus_host,
            milvus_port=vector_config.milvus_port,
            embedding_model=vector_config.embedding_model
        )
        
        # Get existing property IDs in Milvus
        context.log.info("📂 Checking for existing properties in Milvus...")
        existing_ids = set()
        try:
            results = processor.collection.query(
                expr="property_id != ''",
                output_fields=["property_id"],
                limit=16384
            )
            existing_ids = {r['property_id'] for r in results}
            context.log.info(f"Found {len(existing_ids)} existing properties in Milvus")
        except Exception as e:
            context.log.warning(f"Could not load existing IDs: {e}")
        
        # Load from mart table
        context.log.info("📊 Loading properties from Raw table...")
        all_properties = processor.load_from_bigquery()
        
        # Filter out properties already in Milvus
        new_properties = [
            prop for prop in all_properties 
            if prop.get('property_id') not in existing_ids
        ]
        
        context.log.info(f"   Total in Big Query: {len(all_properties)}")
        context.log.info(f"   Already in Milvus: {len(all_properties) - len(new_properties)}")
        context.log.info(f"   New to process: {len(new_properties)}")
        
        if not new_properties:
            context.log.info("✅ No new properties to process!")
            return {
                "total_loaded": len(all_properties),
                "new_processed": 0,
                "already_existed": len(all_properties),
                "timestamp": datetime.now().isoformat(),
                "status": "success_no_new_data"
            }
        
        # Process and store new properties
        stored_count = processor.process_and_store(
            new_properties, 
            batch_size=vector_config.batch_size
        )
        
        # Get updated statistics
        stats = processor.get_collection_stats()
        
        context.log.info("="*60)
        context.log.info("📊 VECTOR PROCESSING SUMMARY")
        context.log.info("="*60)
        context.log.info(f"✅ New properties processed: {stored_count}")
        context.log.info(f"📊 Total in Milvus: {stats}")
        context.log.info("="*60)
        
        return {
            "total_loaded": len(all_properties),
            "new_processed": stored_count,
            "already_existed": len(all_properties) - len(new_properties),
            "total_in_milvus": stats,
            "timestamp": datetime.now().isoformat(),
            "status": "success"
        }
        
    except Exception as e:
        context.log.error(f"❌ Error in vector processing: {str(e)}")
        import traceback
        context.log.error(traceback.format_exc())
        return {
            "total_loaded": 0,
            "new_processed": 0,
            "already_existed": 0,
            "timestamp": datetime.now().isoformat(),
            "status": "failed",
            "error": str(e)
        }


# ============================================================================
# FINAL PIPELINE SUMMARY
# ============================================================================

@asset(
    description="Final summary of complete pipeline (scraping → mart → vectors)",
    group_name="pipeline_summary",
    deps=["scraping_summary", "mart_transformation_summary", "process_to_milvus"]
)
def complete_pipeline_summary(
    context: OpExecutionContext,
    scraping_summary,
    mart_transformation_summary,
    process_to_milvus
):
    """Generate final summary of the entire pipeline"""
    
    summary = {
        "timestamp": datetime.now().isoformat(),
        "scraping": {
            "total_scraped": scraping_summary.get('total_properties_scraped', 0),
            "total_inserted_raw": scraping_summary.get('total_properties_inserted', 0),
            "successful_ops": scraping_summary.get('successful_operations', 0),
            "failed_ops": scraping_summary.get('failed_operations', 0)
        },
        "mart_transformation": {
            "total_rows_mart": mart_transformation_summary.get('mart_total_rows', 0),
            "summary_tables_successful": mart_transformation_summary.get('summary_tables_successful', 0),
            "summary_tables_failed": mart_transformation_summary.get('summary_tables_failed', 0),
            "status": mart_transformation_summary.get('mart_status', 'unknown')
        },
        "vector_processing": {
            "new_processed": process_to_milvus.get('new_processed', 0),
            "total_in_milvus": process_to_milvus.get('total_in_milvus', 0),
            "status": process_to_milvus.get('status', 'unknown')
        },
        "pipeline_status": "success" if (
            scraping_summary.get('failed_operations', 0) == 0 and 
            mart_transformation_summary.get('mart_status') == 'success' and
            process_to_milvus.get('status') in ['success', 'success_no_new_data']
        ) else "partial_failure"
    }
    
    context.log.info("\n" + "="*70)
    context.log.info("🎉 COMPLETE PIPELINE SUMMARY")
    context.log.info("="*70)
    context.log.info(f"📥 Scraping:")
    context.log.info(f"   - Total scraped: {summary['scraping']['total_scraped']}")
    context.log.info(f"   - Inserted to Raw BigQuery: {summary['scraping']['total_inserted_raw']}")
    context.log.info(f"🔄 Mart Transformation:")
    context.log.info(f"   - Total rows in Mart: {summary['mart_transformation']['total_rows_mart']}")
    context.log.info(f"   - Summary tables updated: {summary['mart_transformation']['summary_tables_successful']}/4")
    context.log.info(f"🤖 Vector Processing:")
    context.log.info(f"   - New processed to Milvus: {summary['vector_processing']['new_processed']}")
    context.log.info(f"   - Total in Milvus: {summary['vector_processing']['total_in_milvus']}")
    context.log.info(f"✅ Pipeline Status: {summary['pipeline_status'].upper()}")
    context.log.info("="*70 + "\n")
    
    # Save complete summary
    try:
        with open('C:/Users/MSI/OneDrive/Desktop/prtofolio/real_estate/aqarmap/raw_data/complete_pipeline_summary.json', 'w') as f:
            json.dump(summary, f, indent=2)
        context.log.info("✅ Complete pipeline summary saved")
    except Exception as e:
        context.log.error(f"⚠️ Could not save pipeline summary: {e}")
    
    return summary


# ============================================================================
# DEFINE JOBS
# ============================================================================

# Complete pipeline job
complete_pipeline_job = define_asset_job(
    name="complete_real_estate_pipeline",
    description="Full pipeline",
    selection=[
        # Scraping
        "scrape_alexandria_for_sale",
        "scrape_alexandria_for_rent",
        "scrape_cairo_for_sale",
        "scrape_cairo_for_rent",
        "scraping_summary",
        # Mart transformation
        "property_mart",
        "location_summary",
        "property_type_summary",
        "time_series_summary",
        "price_analysis_summary",
        "data_quality_report",
        "mart_transformation_summary",
        # Vector processing
        "process_to_milvus",
        # Final summary
        "complete_pipeline_summary"
    ]
)

# Scraping only job
scraping_only_job = define_asset_job(
    name="scraping_only",
    description="Only scraping job without transformation or vector processing",
    selection=[
        "scrape_alexandria_for_sale",
        "scrape_alexandria_for_rent",
        "scrape_cairo_for_sale",
        "scrape_cairo_for_rent",
        "scraping_summary"
    ]
)

# Mart transformation only job (for manual runs after scraping)
mart_transformation_only_job = define_asset_job(
    name="mart_transformation_only",
    description="Only transform raw data to mart and update summary tables",
    selection=[
        "property_mart",
        "location_summary",
        "property_type_summary",
        "time_series_summary",
        "price_analysis_summary",
        "data_quality_report",
        "mart_transformation_summary"
    ]
)

# Vector processing only job (for manual runs)
vector_processing_only_job = define_asset_job(
    name="vector_processing_only",
    description="Only process Mart data to Milvus (no scraping or transformation)",
    selection=["process_to_milvus"]
)


# ============================================================================
# SCHEDULES
# ============================================================================

# Main schedule - runs complete pipeline daily at 12 PM
daily_complete_pipeline_schedule = ScheduleDefinition(
    name="daily_complete_pipeline_at_noon",
    job=complete_pipeline_job,
    cron_schedule="0 12 * * *",  # Every day at 12:00 PM
    execution_timezone="Africa/Cairo",
    description="Runs complete pipeline daily at 12:00 PM Cairo time: Scraping → Mart → Vectors"
)

# Mart transformation schedule (runs 2 hours after scraping to ensure data is ready)
mart_transformation_schedule = ScheduleDefinition(
    name="mart_transformation_at_2pm",
    job=mart_transformation_only_job,
    cron_schedule="0 14 * * *",  # Every day at 2:00 PM
    execution_timezone="Africa/Cairo",
    description="Transforms raw data to mart at 2:00 PM Cairo time"
)

# Vector sync schedule (runs every 6 hours to catch any missed data)
vector_sync_schedule = ScheduleDefinition(
    name="vector_sync_every_6_hours",
    job=vector_processing_only_job,
    cron_schedule="0 */6 * * *",  # Every 6 hours
    execution_timezone="Africa/Cairo",
    description="Syncs Mart data to Milvus every 6 hours"
)


# ============================================================================
# DAGSTER DEFINITIONS
# ============================================================================

defs = Definitions(
    assets=[
        # Scraping assets
        scrape_alexandria_for_sale,
        scrape_alexandria_for_rent,
        scrape_cairo_for_sale,
        scrape_cairo_for_rent,
        scraping_summary,
        # Mart transformation assets
        property_mart,
        location_summary,
        property_type_summary,
        time_series_summary,
        price_analysis_summary,
        data_quality_report,
        mart_transformation_summary,
        # Vector processing assets
        process_to_milvus,
        # Final summary
        complete_pipeline_summary
    ],
    jobs=[
        complete_pipeline_job,
        scraping_only_job,
        mart_transformation_only_job,
        vector_processing_only_job
    ],
    schedules=[
        daily_complete_pipeline_schedule,
        mart_transformation_schedule,  
        vector_sync_schedule  
    ],
    resources={
        "scraper_config": ScraperConfig(
            project_id=GCP_PROJECT_ID,
            dataset_id=BQ_DATASET_ID,
            table_id=BQ_TABLE_ID,
            log_file=LOG_FILE,
            max_pages=MAX_PAGES
        ),
        "mart_config": MartConfig(
            project_id=GCP_PROJECT_ID,
            dataset_id_raw=BQ_DATASET_ID,
            table_id_raw=BQ_TABLE_ID,
            dataset_id_mart=BQ_MART_DATASET_ID,
            table_id_mart=BQ_MART_TABLE_ID,
            log_file=MART_LOG_FILE
        ),
        "vector_config": VectorProcessorConfig(
            project_id=GCP_PROJECT_ID,
            dataset_id=BQ_DATASET_ID,  
            table_id=BQ_TABLE_ID,      
            milvus_host=MILVUS_HOST,
            milvus_port=MILVUS_PORT,
            embedding_model=EMBEDDING_MODEL,
            batch_size=VECTOR_BATCH_SIZE
        )
    }
)