"""Mart transformation assets for real estate pipeline"""
from datetime import datetime
import json
from dagster import asset, OpExecutionContext, RetryPolicy
from ...config.settings import config
from ...resources.config_resources import MartResource


@asset(
    description="Transform raw data to property mart table",
    group_name="mart_transformation",
    deps=[
        "scrape_alexandria_for_sale",
        "scrape_alexandria_for_rent", 
        "scrape_cairo_for_sale",
        "scrape_cairo_for_rent"
    ],
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def property_mart(context: OpExecutionContext, mart_resource: MartResource):
    """Transform raw scraped data into cleaned mart table"""
    try:
        context.log.info("🔄 Starting mart table transformation...")
        
        from src.etl.marts_builder.real_estate_mart import PropertyMartBuilder
        from src.databases.big_query.big_query_database import Big_Query_Database
        
        # Initialize database
        db = Big_Query_Database(
            project_id=mart_resource.project_id,
            raw_dataset_id=mart_resource.raw_dataset_id,
            raw_table_id=mart_resource.raw_table_id,
            mart_dataset_id=mart_resource.mart_dataset_id,
            mart_table_id=mart_resource.mart_table_id,
            log_dir=mart_resource.log_dir
        )
        db.connect()
        
        # Initialize mart builder
        mart_builder = PropertyMartBuilder(
            log_dir=mart_resource.log_dir,
            db=db
        )
        
        # Run Create Mart table
        total_rows = mart_builder.create_mart_table()
        
        context.log.info(f"✅ Mart table created with total {total_rows:,} rows")
        
        return {
            "total_rows": total_rows,
            "timestamp": datetime.now().isoformat(),
            "status": "success"
        }
        
    except Exception as e:
        context.log.error(f"❌ Error transforming to mart: {str(e)}")
        import traceback
        context.log.error(traceback.format_exc())
        return {
            "total_rows": 0,
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
def location_summary(context: OpExecutionContext, mart_resource: MartResource):
    """Location summary snapshot"""
    try:
        context.log.info("🗺️ Creating location summary snapshot...")
        
        from src.etl.marts_builder.real_estate_mart import PropertyMartBuilder
        from src.databases.big_query.big_query_database import Big_Query_Database
        
        db = Big_Query_Database(
            project_id=mart_resource.project_id,
            raw_dataset_id=mart_resource.raw_dataset_id,
            raw_table_id=mart_resource.raw_table_id,
            mart_dataset_id=mart_resource.mart_dataset_id,
            mart_table_id=mart_resource.mart_table_id,
            log_dir=mart_resource.log_dir
        )
        db.connect()
        
        mart_builder = PropertyMartBuilder(
            log_dir=mart_resource.log_dir,
            db=db
        )
        
        mart_builder.create_location_summary_mart()
        
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
def property_type_summary(context: OpExecutionContext, mart_resource: MartResource):
    """Creating new property type summary snapshot"""
    try:
        context.log.info("🏠 Creating property type summary snapshot...")
        
        from src.etl.marts_builder.real_estate_mart import PropertyMartBuilder
        from src.databases.big_query.big_query_database import Big_Query_Database
        
        db = Big_Query_Database(
            project_id=mart_resource.project_id,
            raw_dataset_id=mart_resource.raw_dataset_id,
            raw_table_id=mart_resource.raw_table_id,
            mart_dataset_id=mart_resource.mart_dataset_id,
            mart_table_id=mart_resource.mart_table_id,
            log_dir=mart_resource.log_dir
        )
        db.connect()
        
        mart_builder = PropertyMartBuilder(
            log_dir=mart_resource.log_dir,
            db=db
        )
        
        mart_builder.create_property_type_summary_mart()
        
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
def time_series_summary(context: OpExecutionContext, mart_resource: MartResource):
    """Create new time series summary snapshot"""
    try:
        context.log.info("📅 Creating time series summary snapshot...")
        
        from src.etl.marts_builder.real_estate_mart import PropertyMartBuilder
        from src.databases.big_query.big_query_database import Big_Query_Database
        
        db = Big_Query_Database(
            project_id=mart_resource.project_id,
            raw_dataset_id=mart_resource.raw_dataset_id,
            raw_table_id=mart_resource.raw_table_id,
            mart_dataset_id=mart_resource.mart_dataset_id,
            mart_table_id=mart_resource.mart_table_id,
            log_dir=mart_resource.log_dir
        )
        db.connect()
        
        mart_builder = PropertyMartBuilder(
            log_dir=mart_resource.log_dir,
            db=db
        )
        
        mart_builder.create_time_series_summary_mart()
        
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
def price_analysis_summary(context: OpExecutionContext, mart_resource: MartResource):
    """Create new price analysis summary snapshot"""
    try:
        context.log.info("💰 Creating price analysis summary snapshot...")
        
        from src.etl.marts_builder.real_estate_mart import PropertyMartBuilder
        from src.databases.big_query.big_query_database import Big_Query_Database
        
        db = Big_Query_Database(
            project_id=mart_resource.project_id,
            raw_dataset_id=mart_resource.raw_dataset_id,
            raw_table_id=mart_resource.raw_table_id,
            mart_dataset_id=mart_resource.mart_dataset_id,
            mart_table_id=mart_resource.mart_table_id,
            log_dir=mart_resource.log_dir
        )
        db.connect()
        
        mart_builder = PropertyMartBuilder(
            log_dir=mart_resource.log_dir,
            db=db
        )
        
        mart_builder.create_price_analysis_summary_mart()
        
        context.log.info("✅ Price analysis summary snapshot created successfully")
        
        return {
            "snapshot_date": datetime.now().date().isoformat(),
            "timestamp": datetime.now().isoformat(),
            "status": "success"
        }
        
    except Exception as e:
        context.log.error(f"❌ Error creating price analysis summary: {str(e)}")
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
def data_quality_report(context: OpExecutionContext, mart_resource: MartResource):
    """Create new data quality report snapshot"""
    try:
        context.log.info("🔍 Creating data quality report snapshot...")
        
        from src.etl.marts_builder.real_estate_mart import PropertyMartBuilder
        from src.databases.big_query.big_query_database import Big_Query_Database
        
        db = Big_Query_Database(
            project_id=mart_resource.project_id,
            raw_dataset_id=mart_resource.raw_dataset_id,
            raw_table_id=mart_resource.raw_table_id,
            mart_dataset_id=mart_resource.mart_dataset_id,
            mart_table_id=mart_resource.mart_table_id,
            log_dir=mart_resource.log_dir
        )
        db.connect()
        
        mart_builder = PropertyMartBuilder(
            log_dir=mart_resource.log_dir,
            db=db
        )
        
        mart_builder.create_data_quality_report_mart()
        
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