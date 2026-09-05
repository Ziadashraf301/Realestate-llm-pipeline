"""
Centralized Configuration Module powered by Pydantic Settings.
Validates all environment variables, ports, credentials, and endpoints at application startup.
"""

from functools import lru_cache
from typing import Literal
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False
    )

    # Application Environment
    ENVIRONMENT: Literal["development", "staging", "production"] = "development"
    APP_NAME: str = "Real Estate Intelligence System"
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 5000
    DEBUG: bool = False

    # Milvus Vector Database
    MILVUS_HOST: str = "localhost"
    MILVUS_PORT: int = 19530
    MILVUS_COLLECTION_NAME: str = "properties"
    MILVUS_SEMANTIC_CACHE_COLLECTION: str = "semantic_query_cache"

    # Redis (Two-Tier Caching & Rate Limiting)
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: str | None = None
    CACHE_TTL_SECONDS: int = 21600  # 6 Hours
    SEMANTIC_SIMILARITY_THRESHOLD: float = 0.96

    # ClickHouse Warehouse (Local Real-Time Analytics & Replace BigQuery)
    CLICKHOUSE_HOST: str = "localhost"
    CLICKHOUSE_PORT: int = 8123  # HTTP Interface
    CLICKHOUSE_NATIVE_PORT: int = 9002  # Native Interface (mapped from 9000 inside container)
    CLICKHOUSE_USER: str = "default"
    CLICKHOUSE_PASSWORD: str = ""
    CLICKHOUSE_DB: str = "real_estate"

    # MLflow Tracking & Registry (SQLite metadata + MinIO S3 artifacts)
    MLFLOW_TRACKING_URI: str = "http://localhost:5001"
    MLFLOW_EXPERIMENT_NAME: str = "Real_Estate_GenAI_Production"
    MLFLOW_PROMPT_NAME: str = "real_estate_advisor_prompt"
    MLFLOW_INTENT_PROMPT_NAME: str = "real_estate_intent_prompt"
    MLFLOW_ADVISOR_PROMPT_NAME: str = "real_estate_advisor_prompt"
    # MLflow S3-compatible MinIO artifact store
    MLFLOW_S3_ENDPOINT_URL: str = "http://localhost:9000"
    AWS_ACCESS_KEY_ID: str = "minioadmin"
    AWS_SECRET_ACCESS_KEY: str = "minioadmin"

    # Local MinIO Storage (DVC Remote)
    MINIO_ENDPOINT: str = "http://localhost:9000"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin"
    MINIO_DVC_BUCKET: str = "dvc-storage"

    # Native llama.cpp C++ Server (Port 8080)
    LLAMA_CPP_ENDPOINT: str = "http://localhost:8080"
    LLAMA_CPP_MODEL_PATH: str = "models/qwen2.5-1.5b-instruct-q4_k_m.gguf"

    # ONNX Runtime Models (Embeddings & Reranker)
    ONNX_EMBEDDING_MODEL_PATH: str = "models/onnx/multilingual-e5-small-int8.onnx"
    ONNX_RERANKER_MODEL_PATH: str = "models/onnx/bge-reranker-base-int8.onnx"

    # Google Gemini LLM API (Cloud Fallback: e.g. gemini-2.0-flash, gemini-3.1-preview)
    GOOGLE_API_KEY: str | None = None
    GEMINI_MODEL: str = "gemini-2.0-flash"

    # User Authentication & Security (JWT + Bcrypt)
    JWT_SECRET_KEY: str = "change_this_to_a_secure_random_hex_key_in_production"
    JWT_ALGORITHM: str = "HS256"
    JWT_ACCESS_TOKEN_EXPIRE_HOURS: int = 12

    # Rate Limiting & User Quotas (override via env: RATE_LIMIT_REQUESTS_PER_MINUTE)
    RATE_LIMIT_REQUESTS_PER_MINUTE: int = 60  # 60 for dev/demo; set 10 in production env


@lru_cache
def get_settings() -> Settings:
    """Returns a cached singleton instance of Settings."""
    return Settings()


# Export convenient global singleton
settings = get_settings()
