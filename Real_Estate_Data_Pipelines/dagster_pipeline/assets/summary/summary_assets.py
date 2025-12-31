"""Summary assets for real estate pipeline"""
import json
from datetime import datetime
from dagster import asset, AssetExecutionContext, RetryPolicy
from pathlib import Path

from Real_Estate_Data_Pipelines.src.config import config

# Import all scraping assets dynamically
from Real_Estate_Data_Pipelines.dagster_pipeline.assets.scraping.scraping_assets import get_scraping_asset_names

# Import all mart assets dynamically
from Real_Estate_Data_Pipelines.dagster_pipeline.assets.mart.mart_assets import get_mart_asset_names


# Generate dynamic dependencies
scraping_deps = get_scraping_asset_names()
mart_deps = get_mart_asset_names()


@asset(
    description="Summary of all scraping operations - Auto-discovers all scraping assets",
    group_name="real_estate_scraping",
    deps=scraping_deps,
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def scraping_summary(context: AssetExecutionContext):
    """
    Generate summary of all scraping operations.
    Loads results from Dagster's asset storage.
    """

    from Real_Estate_Data_Pipelines.src.helpers import upload_to_s3

    all_results = []
    scraping_asset_names = []
    
    # Get asset names dynamically
    asset_names = get_scraping_asset_names()
    
    # Load each scraping asset's output from storage
    for asset_name in asset_names:
        try:
            # Load the materialized value from the IO manager
            from dagster import AssetKey
            
            # Try to load the asset value
            asset_key = AssetKey([asset_name])
            
            # Get the latest materialization
            materialization = context.instance.get_latest_materialization_event(asset_key)
            
            if materialization and materialization.asset_materialization:
                # Extract metadata
                metadata = materialization.asset_materialization.metadata
                
                # Build result dict from metadata
                result_dict = {
                    'scraped_count': metadata.get('scraped_count').value if metadata.get('scraped_count') else 0,
                    'inserted_count': metadata.get('inserted_count').value if metadata.get('inserted_count') else 0,
                    'status': metadata.get('status').value if metadata.get('status') else 'unknown'
                }
                
                all_results.append(result_dict)
                scraping_asset_names.append(asset_name)
                
                context.log.info(f"✅ Loaded {asset_name}: {result_dict}")
            else:
                context.log.warning(f"⚠️ No materialization found for {asset_name}")
                
        except Exception as e:
            context.log.error(f"❌ Error loading {asset_name}: {e}")
    
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
    
    # Save summary to file
    try:
        output_dir = Path(config.PROJECT_ROOT) / "Real_Estate_Data_Pipelines" / "raw_data" / "summary"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "scraping_summary.json"
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        context.log.info(f"✅ Summary saved to {output_path}")
        upload_to_s3(local_file_path=str(output_path), 
                         s3_key="summary/scraping_summary.json", 
                         logger=context.log, 
                         bucket_name = "real-estate-301")
    except Exception as e:
        context.log.error(f"⚠️ Could not save summary: {e}")
    
    # Return with metadata
    from dagster import Output, MetadataValue
    
    return Output(
        value=summary,
        metadata={
            "total_operations": MetadataValue.int(total_operations),
            "total_properties_scraped": MetadataValue.int(total_scraped),
            "total_properties_inserted": MetadataValue.int(total_inserted),
            "successful_operations": MetadataValue.int(successful),
            "failed_operations": MetadataValue.int(failed)
        }
    )


@asset(
    description="Summary of mart transformation operations - Auto-discovers all mart assets",
    group_name="real_estate_mart",
    deps=mart_deps,
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def mart_transformation_summary(context: AssetExecutionContext):
    """
    Generate summary of all mart transformation operations.
    Loads results from Dagster's asset storage.
    """
    from Real_Estate_Data_Pipelines.src.helpers import upload_to_s3

    all_results = []
    mart_asset_names_list = []
    
    # Get asset names dynamically
    asset_names = get_mart_asset_names()
    
    # Load each mart asset's output from storage
    for asset_name in asset_names:
        try:
            from dagster import AssetKey
            
            asset_key = AssetKey([asset_name])
            materialization = context.instance.get_latest_materialization_event(asset_key)
            
            if materialization and materialization.asset_materialization:
                metadata = materialization.asset_materialization.metadata
                
                result_dict = {
                    'table_name': asset_name,
                    'row_count': metadata.get('row_count').value if metadata.get('row_count') else 0,
                    'status': metadata.get('status').value if metadata.get('status') else 'unknown'
                }
                
                all_results.append(result_dict)
                mart_asset_names_list.append(asset_name)
                
                context.log.info(f"✅ Loaded {asset_name}: {result_dict}")
            else:
                context.log.warning(f"⚠️ No materialization found for {asset_name}")
                
        except Exception as e:
            context.log.error(f"❌ Error loading {asset_name}: {e}")
    
    # Calculate statistics
    context.log.info(all_results)
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
        "operations": mart_asset_names_list,
        "details": all_results
    }
    
    # Log summary
    context.log.info("🔄 MART TRANSFORMATION SUMMARY")
    context.log.info(f"✅ Successful transformations: {successful}/{total_operations}")
    context.log.info(f"❌ Failed transformations: {failed}/{total_operations}")
    context.log.info(f"📊 Total rows processed: {total_rows_processed}")
    context.log.info(f"📋 Tables: {', '.join(mart_asset_names_list)}")
    
    # Save summary
    try:
        output_dir = Path(config.PROJECT_ROOT) / "Real_Estate_Data_Pipelines" / "raw_data" / "summary"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "mart_summary.json"
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        context.log.info(f"✅ Mart summary saved to {output_path}")
        upload_to_s3(local_file_path=str(output_path), 
                         s3_key="summary/mart_summary.json", 
                         logger=context.log, 
                         bucket_name = "real-estate-301")
    except Exception as e:
        context.log.error(f"⚠️ Could not save mart summary: {e}")
    
    # Return with metadata
    from dagster import Output, MetadataValue
    
    return Output(
        value=summary,
        metadata={
            "total_operations": MetadataValue.int(total_operations),
            "total_rows_processed": MetadataValue.int(total_rows_processed),
            "successful_operations": MetadataValue.int(successful),
            "failed_operations": MetadataValue.int(failed)
        }
    )


@asset(
    description="Complete pipeline summary",
    group_name="real_estate_pipeline",
    deps=["scraping_summary", "mart_transformation_summary", "process_to_milvus"],
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def complete_pipeline_summary(context: AssetExecutionContext):
    """
    Generate complete pipeline summary including all stages.
    """
    from Real_Estate_Data_Pipelines.src.helpers import upload_to_s3
    
    # Load summaries from storage
    from dagster import AssetKey
    
    scraping_summary = {}
    mart_transformation_summary = {}
    process_to_milvus_result = {}
    
    try:
        # Load scraping summary
        scraping_event = context.instance.get_latest_materialization_event(
            AssetKey(["scraping_summary"])
        )
        if scraping_event and scraping_event.asset_materialization:
            metadata = scraping_event.asset_materialization.metadata
            scraping_summary = {
                'total_properties_scraped': metadata.get('total_properties_scraped').value if metadata.get('total_properties_scraped') else 0,
                'total_properties_inserted': metadata.get('total_properties_inserted').value if metadata.get('total_properties_inserted') else 0,
                'successful_operations': metadata.get('successful_operations').value if metadata.get('successful_operations') else 0,
                'failed_operations': metadata.get('failed_operations').value if metadata.get('failed_operations') else 0,
            }
        
        # Load mart summary
        mart_event = context.instance.get_latest_materialization_event(
            AssetKey(["mart_transformation_summary"])
        )
        if mart_event and mart_event.asset_materialization:
            metadata = mart_event.asset_materialization.metadata
            mart_transformation_summary = {
                'total_rows_processed': metadata.get('total_rows_processed').value if metadata.get('total_rows_processed') else 0,
                'successful_operations': metadata.get('successful_operations').value if metadata.get('successful_operations') else 0,
                'failed_operations': metadata.get('failed_operations').value if metadata.get('failed_operations') else 0,
            }
        
        # Load vector processing
        vector_event = context.instance.get_latest_materialization_event(
            AssetKey(["process_to_milvus"])
        )
        if vector_event and vector_event.asset_materialization:
            metadata = vector_event.asset_materialization.metadata
            process_to_milvus_result = {
                'processed_count': metadata.get('processed_count').value if metadata.get('processed_count') else 0,
                'total_count': metadata.get('total_count').value if metadata.get('total_count') else 0,
                'failed_count': metadata.get('failed_validations').value if metadata.get('failed_validations') else 0,
                'status': metadata.get('status').value if metadata.get('status') else 'unknown'
            }
            
    except Exception as e:
        context.log.error(f"Error loading summary data: {e}")
    
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
                "processed_count": process_to_milvus_result.get('processed_count', 0),
                "total_in_milvus": process_to_milvus_result.get('total_count', 0),
                "failed_count": process_to_milvus_result.get('failed_count', 0),
                "status": process_to_milvus_result.get('status', 'unknown')
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
        output_dir = Path(config.PROJECT_ROOT) / "Real_Estate_Data_Pipelines" / "raw_data" / "summary"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "complete_pipeline_summary.json"
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        context.log.info(f"✅ Complete pipeline summary saved to {output_path}")
        upload_to_s3(local_file_path=str(output_path), 
                         s3_key="summary/complete_pipeline_summary.json", 
                         logger=context.log, 
                         bucket_name = "real-estate-301")
    except Exception as e:
        context.log.error(f"⚠️ Could not save pipeline summary: {e}")
    
    return summary