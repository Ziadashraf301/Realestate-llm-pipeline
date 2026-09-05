"""Dagster configurable resources for real estate pipeline (Clean Architecture)."""
from dagster import ConfigurableResource
from pydantic import Field
from real_estate.core.settings import settings


class ScraperResource(ConfigurableResource):
    """Resource for scraper configuration"""
    max_pages: int = Field(default=10)
    max_concurrency: int = Field(default=5)


class MartResource(ConfigurableResource):
    """Resource for mart warehouse configuration"""
    clickhouse_host: str = Field(default=settings.CLICKHOUSE_HOST)
    clickhouse_port: int = Field(default=settings.CLICKHOUSE_PORT)
    database: str = Field(default=settings.CLICKHOUSE_DB)


class VectorResource(ConfigurableResource):
    """Resource for vector processor configuration"""
    milvus_host: str = Field(default=settings.MILVUS_HOST)
    milvus_port: int = Field(default=settings.MILVUS_PORT)
    milvus_collection_name: str = Field(default=settings.MILVUS_COLLECTION_NAME)
    batch_size: int = Field(default=500)