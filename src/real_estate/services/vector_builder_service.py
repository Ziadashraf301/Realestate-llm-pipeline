"""
Streaming Vector Builder Service (Enterprise M2 Pipeline).
Streams properties from ClickHouse in batches of 500, generates dense INT8 embeddings
via OnnxEmbeddingService, and flushes directly to Milvus.
Guarantees constant memory consumption (< 150 MB).
"""

import asyncio
from typing import List, Dict, Any, Optional
from real_estate.core.logger import logger
from real_estate.core.settings import settings
from real_estate.retrieval.onnx_embedder import OnnxEmbeddingService
from real_estate.repositories.base import BaseVectorRepository
from real_estate.repositories.warehouse_repository import ClickHouseWarehouseRepository


class StreamingVectorBuilderService:
    """Streams data from columnar store into Milvus vector collection without RAM bloat."""

    def __init__(
        self,
        warehouse: ClickHouseWarehouseRepository,
        vector_repo: BaseVectorRepository,
        embedder: Optional[OnnxEmbeddingService] = None,
        batch_size: int = 500,
        exclude_source: Optional[str] = "admin_api",
        logger: Optional[Any] = None,
    ):
        self.warehouse = warehouse
        self.vector_repo = vector_repo
        self.embedder = embedder or OnnxEmbeddingService()
        self.batch_size = batch_size
        self.exclude_source = exclude_source
        self.dagster_logger = logger

    def _compose_passage_text(self, prop: Dict[str, Any]) -> str:
        """Composes descriptive passage text optimized for dense embedding retrieval."""
        parts = [
            f"العنوان: {prop.get('title', '')}",
            f"الموقع: {prop.get('location', '')}",
            f"النوع: {prop.get('property_type', '')} {prop.get('listing_type', '')}",
            f"السعر: {prop.get('price_egp', 0)} جنيه مصري",
        ]
        if prop.get("bedrooms"):
            parts.append(f"غرف النوم: {prop['bedrooms']}")
        if prop.get("bathrooms"):
            parts.append(f"الحمامات: {prop['bathrooms']}")
        if prop.get("area_sqm"):
            parts.append(f"المساحة: {prop['area_sqm']} متر مربع")
        if prop.get("floor_number"):
            parts.append(f"الطابق: {prop['floor_number']}")
        if prop.get("address"):
            parts.append(f"العنوان التفصيلي: {prop['address']}")
        if prop.get("description"):
            parts.append(f"التفاصيل: {prop['description'][:400]}")

        return " | ".join(parts)

    async def run(self) -> int:
        """Executes generator streaming vector build and Milvus ingestion with batching."""
        start_time = asyncio.get_event_loop().time()
        logger.info("starting_streaming_vector_ingestion", batch_size=self.batch_size, exclude_source=self.exclude_source)
        if self.dagster_logger:
            self.dagster_logger.info(f"🚀 Starting vector streaming into Milvus (batch_size={self.batch_size})...")

        total_indexed = 0
        batch_idx = 0

        async for batch in self.warehouse.stream_properties(batch_size=self.batch_size, exclude_source=self.exclude_source):
            batch_idx += 1
            valid_props = [p for p in batch if p.get("id")]
            if not valid_props:
                continue

            logger.info("processing_vector_batch", batch_num=batch_idx, batch_size=len(batch), current_total=total_indexed)
            if self.dagster_logger:
                self.dagster_logger.info(f"⚡ Encoding Batch #{batch_idx} ({len(valid_props)} listings) with ONNX INT8...")

            try:
                passages = [self._compose_passage_text(p) for p in valid_props]
                # 1. Batch dense embedding (ONNX INT8 in 64-item micro-chunks)
                vector_arrs = await asyncio.to_thread(self.embedder.encode_batch, passages, is_query=False, chunk_size=64)

                if self.dagster_logger:
                    self.dagster_logger.info(
                        f"💾 [Batch #{batch_idx}] Upserting {len(valid_props)} vectors & metadata payloads into Milvus..."
                    )

                items_to_upsert = []
                for prop, passage_text, vec_arr in zip(valid_props, passages, vector_arrs):
                    prop_id = prop["id"]
                    payload = {
                        "title": prop.get("title", ""),
                        "location": prop.get("location", ""),
                        "price_egp": float(prop.get("price_egp", 0.0)),
                        "listing_type": prop.get("listing_type", ""),
                        "property_type": prop.get("property_type", ""),
                        "bedrooms": prop.get("bedrooms"),
                        "bathrooms": prop.get("bathrooms"),
                        "area_sqm": prop.get("area_sqm"),
                        "text": passage_text,
                        "url": prop.get("url", ""),
                    }
                    items_to_upsert.append({
                        "id": prop_id,
                        "vector": vec_arr.tolist(),
                        "payload": payload,
                    })

                # 2. Bulk upsert to Milvus
                if hasattr(self.vector_repo, "upsert_property_vectors_batch"):
                    await self.vector_repo.upsert_property_vectors_batch(items_to_upsert)
                else:
                    for item in items_to_upsert:
                        await self.vector_repo.upsert_property_vector(
                            property_id=str(item["id"]),
                            vector=[float(x) for x in item["vector"]],
                            payload=dict(item["payload"]),
                        )
                total_indexed += len(items_to_upsert)
                if self.dagster_logger:
                    self.dagster_logger.info(
                        f"✅ [Batch #{batch_idx}] Successfully indexed {len(items_to_upsert)} listings (Total Indexed: {total_indexed})"
                    )
            except Exception as e:
                logger.error("error_indexing_property_vector_batch", batch_size=len(valid_props), error=str(e))
                if self.dagster_logger:
                    self.dagster_logger.error(f"❌ [Batch #{batch_idx}] Failed: {str(e)}")

        elapsed_sec = asyncio.get_event_loop().time() - start_time
        logger.info("streaming_vector_ingestion_complete", total_indexed=total_indexed, duration_seconds=round(elapsed_sec, 2))

        return total_indexed


# Backward-compatible alias
StreamingVectorBuilder = StreamingVectorBuilderService
