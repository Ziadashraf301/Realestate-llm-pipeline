"""Vector processing assets for real estate pipeline"""
from datetime import datetime
from dagster import asset, OpExecutionContext, RetryPolicy
from ...config.settings import config
from ...resources.config_resources import VectorResource


@asset(
    description="Process mart data to Milvus vector database",
    group_name="vector_processing",
    deps=["property_mart"],
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def process_to_milvus(context: OpExecutionContext, vector_resource: VectorResource):
    """Process properties from Mart table and store in Milvus"""
    try:
        context.log.info("🤖 Starting vector processing from mart...")
        
        from src.etl import PropertyVectorBuilder
        from src.databases import Big_Query_Database
        from src.databases import Milvus_VectorDatabase
        from src.helpers import EmbeddingService
        from src.helpers import TextPreprocessor
        
        # Initialize components
        embedding_service = EmbeddingService(
            model_name=vector_resource.embedding_model,
            log_dir=vector_resource.log_dir
        )
        
        bigquery_client = Big_Query_Database(
            project_id=vector_resource.project_id,
            mart_dataset_id=vector_resource.mart_dataset_id,
            mart_table_id=vector_resource.mart_table_id,
            log_dir=vector_resource.log_dir
        )
        bigquery_client.connect()
        
        milvus_client = Milvus_VectorDatabase(
            log_dir=vector_resource.log_dir,
            milvus_host=vector_resource.milvus_host,
            milvus_port=vector_resource.milvus_port,
            collection_name=vector_resource.milvus_collection_name,
            embedding_dim=vector_resource.embedding_dim
        )
        milvus_client.connect()
        
        # Create collection if not exists
        milvus_client.create_collection()
        
        text_preprocessor = TextPreprocessor()
        
        # Initialize pipeline
        pipeline = PropertyVectorBuilder(
            rdbms_client=bigquery_client,
            vectordb_client=milvus_client,
            text_preprocessor=text_preprocessor,
            embedding_service=embedding_service,
            log_dir=vector_resource.log_dir
        )
        
        # Execute pipeline
        results = pipeline.process_store_to_vdb(
            batch_size=vector_resource.batch_size
        )
        
        # Get collection stats
        stats = milvus_client.get_collection_stats()
        
        context.log.info("📊 VECTOR PROCESSING SUMMARY")
        context.log.info(f"✅ Total properties processed: {results['total']:,}")
        context.log.info(f"✅ Successfully inserted: {results['inserted']:,}")
        context.log.info(f"❌ Failed validations: {results['failed']:,}")
        context.log.info(f"📊 Total in Milvus: {stats}")
        
        return {
            "total_processed": results['total'],
            "new_inserted": results['inserted'],
            "failed_validations": results['failed'],
            "total_in_milvus": stats,
            "timestamp": datetime.now().isoformat(),
            "status": "success"
        }
        
    except Exception as e:
        context.log.error(f"❌ Error in vector processing: {str(e)}")
        import traceback
        context.log.error(traceback.format_exc())
        return {
            "total_processed": 0,
            "new_inserted": 0,
            "failed_validations": 0,
            "timestamp": datetime.now().isoformat(),
            "status": "failed",
            "error": str(e)
        }