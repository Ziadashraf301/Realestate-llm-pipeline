"""
Vector processing assets for Dagster (Clean Architecture).
Extracts from ClickHouse mart, encodes with ONNX INT8, and upserts to Milvus HNSW.
"""

import asyncio
from datetime import datetime
from dagster import asset, OpExecutionContext, RetryPolicy, Output, MetadataValue

from real_estate.core.logger import logger
from real_estate.repositories.warehouse_repository import ClickHouseWarehouseRepository
from real_estate.repositories.vector_repository import MilvusVectorRepository
from real_estate.services.vector_builder_service import StreamingVectorBuilderService
from real_estate.pipelines.resources.config_resources import VectorResource


@asset(
    description="Streams ClickHouse property mart into Milvus dense vector collection with ONNX INT8 embeddings",
    group_name="vector_processing",
    deps=["property_mart"],
    retry_policy=RetryPolicy(max_retries=2, delay=120)
)
def process_to_milvus(context: OpExecutionContext, vector_resource: VectorResource) -> Output[dict]:
    """Streams data from ClickHouse into Milvus keeping memory strictly under 150MB."""
    context.log.info("🤖 Starting Streaming Vector Processing into Milvus...")

    warehouse = ClickHouseWarehouseRepository(logger=context.log)
    vector_repo = MilvusVectorRepository(logger=context.log)
    vector_repo.initialize_collection()

    builder = StreamingVectorBuilderService(
        warehouse=warehouse,
        vector_repo=vector_repo,
        batch_size=vector_resource.batch_size,
        logger=context.log,
    )

    try:
        total_indexed = asyncio.run(builder.run())
        context.log.info(f"✅ Successfully indexed {total_indexed} properties into Milvus vector collection.")

        return Output(
            value={
                "total_indexed": total_indexed,
                "timestamp": datetime.utcnow().isoformat(),
                "status": "success"
            },
            metadata={
                "vectors_indexed": MetadataValue.int(total_indexed),
                "embedding_model": MetadataValue.text("multilingual-e5-small (INT8)"),
                "vector_db": MetadataValue.text("Milvus Standalone (HNSW)"),
                "status": MetadataValue.text("success")
            }
        )
    except Exception as e:
        context.log.error(f"❌ Error during Milvus vector processing: {str(e)}")
        raise