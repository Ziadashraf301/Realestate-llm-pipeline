"""Summary assets for real estate pipeline"""
import json
from datetime import datetime
from dagster import asset, OpExecutionContext
from ...config.settings import config


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
    
    context.log.info("📊 SCRAPING SUMMARY")
    context.log.info(f"✅ Successful operations: {successful}/4")
    context.log.info(f"❌ Failed operations: {failed}/4")
    context.log.info(f"📈 Total properties scraped: {total_scraped}")
    context.log.info(f"💾 Total properties inserted to BigQuery: {total_inserted}")
    
    try:
        output_path = config.PROJECT_ROOT / "raw_data" / "scraping_summary.json"
        with open(output_path, 'w') as f:
            json.dump(summary, f, indent=2)
        context.log.info(f"✅ Summary saved to {output_path}")
    except Exception as e:
        context.log.error(f"⚠️ Could not save summary: {e}")
    
    return summary


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
    
    context.log.info("📊 MART TRANSFORMATION SUMMARY")
    context.log.info(f"✅ Mart table: {property_mart.get('total_rows', 0):,} total rows")
    context.log.info(f"✅ Successful summary tables: {successful}/5")
    context.log.info(f"❌ Failed summary tables: {failed}/5")
    
    try:
        output_path = config.PROJECT_ROOT / "raw_data" / "mart_summary.json"
        with open(output_path, 'w') as f:
            json.dump(summary, f, indent=2)
        context.log.info(f"✅ Mart summary saved to {output_path}")
    except Exception as e:
        context.log.error(f"⚠️ Could not save mart summary: {e}")
    
    return summary


@asset(
    description="Final summary of complete pipeline",
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
            "new_processed": process_to_milvus.get('new_inserted', 0),
            "total_in_milvus": process_to_milvus.get('total_in_milvus', 0),
            "status": process_to_milvus.get('status', 'unknown')
        },
        "pipeline_status": "success" if (
            scraping_summary.get('failed_operations', 0) == 0 and 
            mart_transformation_summary.get('mart_status') == 'success' and
            process_to_milvus.get('status') == 'success'
        ) else "partial_failure"
    }
    
    context.log.info("🎉 COMPLETE PIPELINE SUMMARY")
    context.log.info(f"📥 Scraping:")
    context.log.info(f"   - Total scraped: {summary['scraping']['total_scraped']}")
    context.log.info(f"   - Inserted to Raw BigQuery: {summary['scraping']['total_inserted_raw']}")
    context.log.info(f"🔄 Mart Transformation:")
    context.log.info(f"   - Total rows in Mart: {summary['mart_transformation']['total_rows_mart']:,}")
    context.log.info(f"   - Summary tables updated: {summary['mart_transformation']['summary_tables_successful']}/5")
    context.log.info(f"🤖 Vector Processing:")
    context.log.info(f"   - New processed to Milvus: {summary['vector_processing']['new_processed']}")
    context.log.info(f"   - Total in Milvus: {summary['vector_processing']['total_in_milvus']}")
    context.log.info(f"✅ Pipeline Status: {summary['pipeline_status'].upper()}")
    
    # Save complete summary
    try:
        output_path = config.PROJECT_ROOT / "raw_data" / "complete_pipeline_summary.json"
        with open(output_path, 'w') as f:
            json.dump(summary, f, indent=2)
        context.log.info(f"✅ Complete pipeline summary saved to {output_path}")
    except Exception as e:
        context.log.error(f"⚠️ Could not save pipeline summary: {e}")
    
    return summary