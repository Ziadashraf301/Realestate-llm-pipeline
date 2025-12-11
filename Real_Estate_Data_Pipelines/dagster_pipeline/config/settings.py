"""Configuration management for Dagster pipeline"""
import os
import json
from pathlib import Path
from typing import Dict, Any
from pydantic import BaseModel, Field


class PipelineConfig(BaseModel):
    """Pipeline configuration loaded from JSON"""
    # GCP Configuration
    GCP_PROJECT_ID: str
    BQ_RAW_DATASET_ID: str
    BQ_RAW_TABLE_ID: str
    BQ_MART_DATASET_ID: str
    BQ_MART_TABLE_ID: str
    
    # Scraping Configuration
    MAX_PAGES: int = 1
    LOG_DIR: str = "logs"
    
    # Milvus Configuration
    MILVUS_HOST: str = "localhost"
    MILVUS_PORT: str = "19530"
    MILVUS_COLLECTION_NAME: str = "real_estate_vectors"
    EMBEDDING_MODEL: str = "all-MiniLM-L6-v2"
    GENERATION_MODEL: str
    EMBEDDING_DIM: int = 384
    BATCH_SIZE: int = 100
    
    # Paths
    PROJECT_ROOT: Path = Field(default_factory=lambda: Path(__file__).resolve().parents[3])
    CONFIG_DIR: Path = Field(default_factory=lambda: Path(__file__).resolve().parents[3] / "Configs")
    SERVICE_ACCOUNT_PATH: Path = Field(default_factory=lambda: Path(__file__).resolve().parents[3] / "Configs" / "big_query_service_account.json")
    
    class Config:
        arbitrary_types_allowed = True


def load_config() -> PipelineConfig:
    """Load configuration from JSON file"""
    config_path = Path(__file__).resolve().parents[3] / "Configs" / "Real_Estate_Data_Pipelines.json"
    
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        config_data = json.load(f)
    
    # Set environment variable for GCP authentication
    service_account_path = Path(__file__).resolve().parents[3] / "Configs" / "big_query_service_account.json"
    if service_account_path.exists():
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(service_account_path)
    
    return PipelineConfig(**config_data)


# Global configuration instance
config = load_config()