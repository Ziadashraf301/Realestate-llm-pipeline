"""Configuration management for the pipeline"""
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
        
    AWS_ACCESS_KEY_ID: str = "",
    AWS_SECRET_ACCESS_KEY: str =  "",
    AWS_REGION: str =  ""

    GEMINI_API_KEY: str = ""

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

    # Set AWS environment variables if provided
    if "AWS_ACCESS_KEY_ID" in config_data and config_data["AWS_ACCESS_KEY_ID"]:
        os.environ['AWS_ACCESS_KEY_ID'] = config_data["AWS_ACCESS_KEY_ID"]
    if "AWS_SECRET_ACCESS_KEY" in config_data and config_data["AWS_SECRET_ACCESS_KEY"]:
        os.environ['AWS_SECRET_ACCESS_KEY'] = config_data["AWS_SECRET_ACCESS_KEY"]
    if "AWS_REGION" in config_data and config_data["AWS_REGION"]:
        os.environ['AWS_DEFAULT_REGION'] = config_data["AWS_REGION"]

    if "GEMINI_API_KEY" in config_data and config_data["GEMINI_API_KEY"]:
        os.environ['GEMINI_API_KEY'] = config_data["GEMINI_API_KEY"]

    return PipelineConfig(**config_data)


# Global configuration instance
config = load_config()