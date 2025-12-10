"""
Real Estate Vector Pipeline Test
Tests the complete ETL pipeline: BigQuery -> Text Processing -> Embeddings -> Milvus
"""

import os

import warnings
from pathlib import Path
from src.config import Config
from src.logger import LoggerFactory
from src.databases import Big_Query_Database, Milvus_VectorDatabase
from src.etl import PropertyVectorBuilder
from src.helpers import TextPreprocessor
from src.helpers import EmbeddingService


def test_vector_pipeline_operations():
    """Execute vector ETL pipeline with structured logging"""

    # Configuration Setup    
    PROJECT_ROOT = Path(__file__).resolve().parents[3]
    CONFIG_DIR = PROJECT_ROOT / "Configs"
    CONFIG_PATH = CONFIG_DIR / "Real_Estate_Data_Pipelines.json"
    GOOGLE_APPLICATION_CREDENTIALS = CONFIG_DIR / "big_query_service_account.json"

    # Load config
    cfg = Config(CONFIG_PATH)

    # Initialize logger
    logger = LoggerFactory.create_logger(log_dir=cfg.LOG_DIR)

    logger.info("""
    ===============================================================
        Real Estate Vector Pipeline v1.0
        ETL: BigQuery -> Text Processing -> Embeddings -> Milvus
    ===============================================================
    """)

    warnings.filterwarnings("ignore")

    # Set environment variables
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(GOOGLE_APPLICATION_CREDENTIALS)

    logger.info(f"Configuration loaded from {CONFIG_PATH}")
    logger.info("")

    # Initialize Components

    logger.info("Initializing pipeline components...")
    logger.info("")

    # Embedding Service
    logger.info(f"Loading embedding model: {cfg.EMBEDDING_MODEL}...")
    embedding_service = EmbeddingService(
        model_name=cfg.EMBEDDING_MODEL,
        log_dir=cfg.LOG_DIR,
    )
    
    # BigQuery Client
    logger.info("Connecting to BigQuery...")
    bigquery_client = Big_Query_Database(
        project_id=cfg.GCP_PROJECT_ID,
        raw_dataset_id=cfg.BQ_RAW_DATASET_ID,
        raw_table_id=cfg.BQ_RAW_TABLE_ID,
        mart_dataset_id=cfg.BQ_MART_DATASET_ID,
        mart_table_id=cfg.BQ_MART_TABLE_ID,
        log_dir=cfg.LOG_DIR,
    )
    bigquery_client.connect()

    # Milvus Client
    logger.info("Connecting to Milvus...")
    milvus_client = Milvus_VectorDatabase(
        log_dir=cfg.LOG_DIR,
        milvus_host=cfg.MILVUS_HOST,
        milvus_port=cfg.MILVUS_PORT,
        collection_name=cfg.MILVUS_COLLECTION_NAME,
        embedding_dim=cfg.EMBEDDING_DIM
    )
    milvus_client.connect()
    
    # Create collection if not exists
    logger.info("Checking Milvus collection...")
    milvus_client.delete_collection()
    milvus_client.create_collection()
    logger.info("")

    # Text Preprocessor
    logger.info("Initializing text preprocessor...")
    text_preprocessor = TextPreprocessor()

    logger.info("")

    # Vector Pipeline
    logger.info("Initializing vector pipeline orchestrator...")
    pipeline = PropertyVectorBuilder(
        rdbms_client=bigquery_client,
        vectordb_client=milvus_client,
        text_preprocessor=text_preprocessor,
        embedding_service=embedding_service,
        log_dir=cfg.LOG_DIR
    )
    logger.info("")

    # Pipeline Configuration

    logger.info("Pipeline Configuration:")
    logger.info(f"  BigQuery Project: {cfg.GCP_PROJECT_ID}")
    logger.info(f"  Mart Dataset: {cfg.BQ_MART_DATASET_ID}")
    logger.info(f"  Mart Table: {cfg.BQ_MART_TABLE_ID}")
    logger.info(f"  Milvus Host: {cfg.MILVUS_HOST}:{cfg.MILVUS_PORT}")
    logger.info(f"  Collection: {cfg.MILVUS_COLLECTION_NAME}")
    logger.info(f"  Embedding Model: {cfg.EMBEDDING_MODEL}")
    logger.info(f"  Embedding Dimension: {cfg.EMBEDDING_DIM}")
    logger.info(f"  Batch Size: {cfg.BATCH_SIZE}")
    logger.info("")

    # Execute Pipeline

    logger.info("Starting vector ETL pipeline...")
    logger.info("")

    try:
        # Run pipeline for testing
        results = pipeline.process_store_to_vdb(
            batch_size=cfg.BATCH_SIZE
        )

        logger.info("Pipeline Execution Summary")
        logger.info(f"Total properties processed: {results['total']:,}")
        logger.info(f"Successfully inserted: {results['inserted']:,}")
        logger.info(f"Failed validations: {results['failed']:,}")
        
        if results['total'] > 0:
            success_rate = (results['inserted'] / results['total']) * 100
            logger.info(f"Success rate: {success_rate:.1f}%")
        
        logger.info("")

        # Verify insertion
        logger.info("Verifying Milvus collection stats...")
        count = milvus_client.get_collection_stats()
        logger.info("")


        logger.info("Vector pipeline completed successfully")

    except KeyboardInterrupt:
        logger.warning("Pipeline manually interrupted")
        logger.info("Closing connections...")

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise

    finally:
        # Cleanup
        logger.info("Closing database connections...")
        milvus_client.close()
        logger.info("All connections closed")


if __name__ == "__main__":
    test_vector_pipeline_operations()