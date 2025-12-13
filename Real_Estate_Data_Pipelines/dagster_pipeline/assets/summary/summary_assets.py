"""Summary assets for real estate pipeline"""
import json
from datetime import datetime
from dagster import asset, AssetExecutionContext, RetryPolicy

from src.config import config

# Import all scraping assets dynamically
from ..scraping.scraping_assets import scraping_assets

# Import all mart assets dynamically
from ..mart.mart_assets import mart_assets

# Generate dynamic dependencies for scraping_summary
scraping_deps = [str(asset_def.key.path[-1]) for asset_def in scraping_assets]

@asset(
    description="Summary of all scraping operations - Auto-discovers all scraping assets",
    group_name="real_estate_scraping",
    deps=scraping_deps,
    retry_policy=RetryPolicy(max_retries=2, delay=300)

)
def scraping_summary(context: AssetExecutionContext, **upstream_assets):
    """
    Generate summary of all scraping operations.
    Automatically discovers and summarizes ALL scraping assets.
    """
    
    # Filter to get only scraping results (dicts with expected keys)
    all_results = []
    scraping_asset_names = []
    
    for asset_name, asset_value in upstream_assets.items():
        # Check if it's a scraping asset result
        if isinstance(asset_value, dict) and ('scraped_count' in asset_value or 'inserted_count' in asset_value):
            all_results.append(asset_value)
            scraping_asset_names.append(asset_name)
    
    # Calculate summary statistics
    total_scraped = sum(r.get('scraped_count', 0) for r in all_results)
    total_inserted = sum(r.get('inserted_count', 0) for r in all_results)
    successful = sum(1 for r in all_results if r.get('status') == 'success')
    failed = sum(1 for r in all_results if r.get('status') == 'failed')
    total_operations = len(all_results)
    
    summary = {
        "timestamp": datetime.now().isoformat(),
        "total_operations": total_operations,
        "total_properties_scraped": total_scraped,
        "total_properties_inserted": total_inserted,
        "successful_operations": successful,
        "failed_operations": failed,
        "operations": scraping_asset_names,
        "details": all_results
    }
    
    # Log summary
    context.log.info("📊 SCRAPING SUMMARY")
    context.log.info(f"✅ Successful operations: {successful}/{total_operations}")
    context.log.info(f"❌ Failed operations: {failed}/{total_operations}")
    context.log.info(f"📈 Total properties scraped: {total_scraped}")
    context.log.info(f"💾 Total properties inserted to BigQuery: {total_inserted}")
    context.log.info(f"📋 Operations: {', '.join(scraping_asset_names)}")
    
    try:
        output_path = config.PROJECT_ROOT / "Real_Estate_Data_Pipelines" / "raw_data" / "scraping_summary.json"
        with open(output_path, 'w') as f:
            json.dump(summary, f, indent=2)
        context.log.info(f"✅ Summary saved to {output_path}")
    except Exception as e:
        context.log.error(f"⚠️ Could not save summary: {e}")
    
    return summary


# Generate dynamic dependencies for mart_transformation_summary
mart_deps = [str(asset_def.key.path[-1]) for asset_def in mart_assets]


@asset(
    description="Summary of mart transformation operations - Auto-discovers all mart assets",
    group_name="real_estate_mart",
    deps=mart_deps,
    retry_policy=RetryPolicy(max_retries=2, delay=300)

)
def mart_transformation_summary(context: AssetExecutionContext, **upstream_assets):
    """
    Generate summary of all mart transformation operations.
    Automatically discovers and summarizes ALL mart assets.
    """
    
    # Filter to get only mart results
    all_results = []
    mart_asset_names = []
    
    for asset_name, asset_value in upstream_assets.items():
        # Check if it's a mart asset result (exclude scraping_summary)
        if isinstance(asset_value, dict) and asset_name != 'scraping_summary':
            if 'row_count' in asset_value or 'table_name' in asset_value:
                all_results.append(asset_value)
                mart_asset_names.append(asset_name)
    
    # Calculate statistics
    total_rows_processed = sum(r.get('row_count', 0) for r in all_results)
    successful = sum(1 for r in all_results if r.get('status') == 'success')
    failed = sum(1 for r in all_results if r.get('status') == 'failed')
    total_operations = len(all_results)
    
    summary = {
        "timestamp": datetime.now().isoformat(),
        "total_operations": total_operations,
        "total_rows_processed": total_rows_processed,
        "successful_operations": successful,
        "failed_operations": failed,
        "operations": mart_asset_names,
        "details": all_results
    }
    
    # Log summary
    context.log.info("🔄 MART TRANSFORMATION SUMMARY")
    context.log.info(f"✅ Successful transformations: {successful}/{total_operations}")
    context.log.info(f"❌ Failed transformations: {failed}/{total_operations}")
    context.log.info(f"📊 Total rows processed: {total_rows_processed}")
    context.log.info(f"📋 Tables: {', '.join(mart_asset_names)}")
    
    try:
        output_path = config.PROJECT_ROOT / "Real_Estate_Data_Pipelines" / "raw_data" / "mart_summary.json"
        with open(output_path, 'w') as f:
            json.dump(summary, f, indent=2)
        context.log.info(f"✅ Mart summary saved to {output_path}")
    except Exception as e:
        context.log.error(f"⚠️ Could not save mart summary: {e}")
    
    return summary


@asset(
    description="Complete pipeline summary",
    group_name="real_estate_pipeline",
    deps=["scraping_summary", "mart_transformation_summary", "process_to_milvus"],
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def complete_pipeline_summary(
    context: AssetExecutionContext,
    scraping_summary: dict,
    mart_transformation_summary: dict,
    process_to_milvus: dict
):
    """
    Generate complete pipeline summary including all stages.
    """
    
    # Aggregate data from all stages
    summary = {
        "timestamp": datetime.now().isoformat(),
        "pipeline_status": "success",
        "stages": {
            "scraping": {
                "total_scraped": scraping_summary.get('total_properties_scraped', 0),
                "total_inserted": scraping_summary.get('total_properties_inserted', 0),
                "successful_ops": scraping_summary.get('successful_operations', 0),
                "failed_ops": scraping_summary.get('failed_operations', 0)
            },
            "mart_transformation": {
                "total_rows": mart_transformation_summary.get('total_rows_processed', 0),
                "successful_ops": mart_transformation_summary.get('successful_operations', 0),
                "failed_ops": mart_transformation_summary.get('failed_operations', 0)
            },
            "vector_processing": {
                "processed_count": process_to_milvus.get('processed_count', 0),
                "total_in_milvus": process_to_milvus.get('total_count', 0),
                "status": process_to_milvus.get('status', 'unknown')
            }
        }
    }
    
    # Determine overall pipeline status
    if (summary['stages']['scraping']['failed_ops'] > 0 or
        summary['stages']['mart_transformation']['failed_ops'] > 0 or
        summary['stages']['vector_processing']['status'] == 'failed'):
        summary['pipeline_status'] = 'partial_failure'
    
    # Log complete summary
    context.log.info("🎉 COMPLETE PIPELINE SUMMARY")
    context.log.info("📥 Scraping:")
    context.log.info(f"   - Total scraped: {summary['stages']['scraping']['total_scraped']}")
    context.log.info(f"   - Inserted to Raw BigQuery: {summary['stages']['scraping']['total_inserted']}")
    
    context.log.info("🔄 Mart Transformation:")
    context.log.info(f"   - Total rows in Mart: {summary['stages']['mart_transformation']['total_rows']}")
    context.log.info(f"   - Summary tables updated: {summary['stages']['mart_transformation']['successful_ops']}")
    
    context.log.info("🤖 Vector Processing:")
    context.log.info(f"   - New processed to Milvus: {summary['stages']['vector_processing']['processed_count']}")
    context.log.info(f"   - Total in Milvus: {summary['stages']['vector_processing']['total_in_milvus']}")
    
    context.log.info(f"✅ Pipeline Status: {summary['pipeline_status'].upper()}")

    # Save complete summary
    try:
        output_path = config.PROJECT_ROOT / "Real_Estate_Data_Pipelines" / "raw_data" / "complete_pipeline_summary.json"
        with open(output_path, 'w') as f:
            json.dump(summary, f, indent=2)
        context.log.info(f"✅ Complete pipeline summary saved to {output_path}")
    except Exception as e:
        context.log.error(f"⚠️ Could not save pipeline summary: {e}")
    
    return summary