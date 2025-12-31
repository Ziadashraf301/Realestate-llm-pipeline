"""Dagster configurable resources"""
from dagster import ConfigurableResource
from pydantic import Field
from Real_Estate_Data_Pipelines.src.config import config

class ScraperResource(ConfigurableResource):
    """Resource for scraper configuration"""
    project_id: str = Field(default=config.GCP_PROJECT_ID)
    raw_dataset_id: str = Field(default=config.BQ_RAW_DATASET_ID)
    raw_table_id: str = Field(default=config.BQ_RAW_TABLE_ID)
    log_dir: str = Field(default=config.LOG_DIR)
    max_pages: int = Field(default=config.MAX_PAGES)


class MartResource(ConfigurableResource):
    """Resource for mart builder configuration"""
    project_id: str = Field(default=config.GCP_PROJECT_ID)
    raw_dataset_id: str = Field(default=config.BQ_RAW_DATASET_ID)
    raw_table_id: str = Field(default=config.BQ_RAW_TABLE_ID)
    mart_dataset_id: str = Field(default=config.BQ_MART_DATASET_ID)
    mart_table_id: str = Field(default=config.BQ_MART_TABLE_ID)
    log_dir: str = Field(default=config.LOG_DIR)


class VectorResource(ConfigurableResource):
    """Resource for vector processor configuration"""
    project_id: str = Field(default=config.GCP_PROJECT_ID)
    mart_dataset_id: str = Field(default=config.BQ_MART_DATASET_ID)
    mart_table_id: str = Field(default=config.BQ_MART_TABLE_ID)
    milvus_host: str = Field(default=config.MILVUS_HOST)
    milvus_port: str = Field(default=config.MILVUS_PORT)
    milvus_collection_name: str = Field(default=config.MILVUS_COLLECTION_NAME)
    embedding_model: str = Field(default=config.EMBEDDING_MODEL)
    embedding_dim: int = Field(default=config.EMBEDDING_DIM)
    batch_size: int = Field(default=config.BATCH_SIZE)
    log_dir: str = Field(default=config.LOG_DIR)