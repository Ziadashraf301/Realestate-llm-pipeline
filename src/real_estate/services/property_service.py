"""
Property Application Service (Clean Architecture: Service Layer).
Coordinates property lifecycle, vector synchronization, and two-tier cache invalidation.
Ensures controllers remain purely responsible for HTTP transport.
"""

import asyncio
from typing import List, Optional
from real_estate.core.logger import logger
from real_estate.repositories.base import BasePropertyRepository, BaseVectorRepository
from real_estate.repositories.warehouse_repository import ClickHouseWarehouseRepository
from real_estate.services.cache_service import TwoTierCacheService
from real_estate.retrieval.onnx_embedder import OnnxEmbeddingService
from real_estate.schemas.property import PropertyCreate, PropertyRead, PropertyUpdate


class PropertyService:
    """Coordinates property domain business logic, vector indexing, warehouse sync, and cache invalidation."""

    def __init__(
        self,
        property_repo: BasePropertyRepository,
        vector_repo: BaseVectorRepository,
        cache_service: TwoTierCacheService,
        embedder: Optional[OnnxEmbeddingService] = None,
        warehouse_repo: Optional[ClickHouseWarehouseRepository] = None,
    ):
        self.property_repo = property_repo
        self.vector_repo = vector_repo
        self.cache_service = cache_service
        self.embedder = embedder or OnnxEmbeddingService()
        self.warehouse_repo = warehouse_repo

    def _build_passage(self, prop: PropertyRead) -> str:
        """Builds rich text passage for dense embedding indexing."""
        parts = [prop.title]
        if prop.location:
            parts.append(f"الموقع: {prop.location}")
        if prop.property_type:
            parts.append(f"النوع: {prop.property_type}")
        if prop.listing_type:
            parts.append(f"{prop.listing_type}")
        if prop.price_egp:
            parts.append(f"السعر: {prop.price_egp:,.0f} جنيه")
        if prop.bedrooms:
            parts.append(f"{prop.bedrooms} غرف")
        if prop.area_sqm:
            parts.append(f"المساحة: {prop.area_sqm} م²")
        if prop.description:
            parts.append(prop.description[:400])
        return " | ".join(parts)

    async def list_properties(self, limit: int = 20, offset: int = 0) -> List[PropertyRead]:
        """Lists properties from repository."""
        return await self.property_repo.list_properties(limit=limit, offset=offset)

    async def get_property(self, property_id: str) -> Optional[PropertyRead]:
        """Retrieves a single property by ID."""
        return await self.property_repo.get_by_id(property_id)

    async def create_property(self, property_in: PropertyCreate) -> PropertyRead:
        """
        Creates a property in Redis, generates dense embedding, upserts into Milvus,
        synchronizes to ClickHouse warehouse (for BI/ML), and invalidates search caches.
        """
        created = await self.property_repo.create(property_in)

        # 1. Sync to Milvus 2.5 Hybrid Vector DB (Dense ONNX + Sparse BM25)
        try:
            passage = self._build_passage(created)
            vector_arr = await asyncio.to_thread(self.embedder.encode, passage, is_query=False)
            vector = vector_arr.tolist()
            await self.vector_repo.upsert_property_vector(
                property_id=created.id,
                vector=vector,
                payload={
                    **created.model_dump(),
                    "text": passage,
                }
            )
            logger.info("property_vector_synced", property_id=created.id)
        except Exception as e:
            logger.warning(
                "milvus_vector_sync_failed_non_fatal",
                property_id=created.id,
                error=str(e)
            )

        # 2. Sync to ClickHouse OLAP Warehouse (Analytics, BI, ML)
        if self.warehouse_repo:
            try:
                ch_record = {
                    "id": created.id,
                    "source": "admin_api",
                    "ingested_from": "property_backend",
                    "title": created.title,
                    "location": created.location,
                    "listing_type": created.listing_type,
                    "property_type": created.property_type,
                    "price_egp": created.price_egp,
                    "bedrooms": created.bedrooms,
                    "bathrooms": created.bathrooms,
                    "area_sqm": created.area_sqm,
                    "description": created.description or "",
                    "url": created.url or f"https://realestate.internal/properties/{created.id}",
                }
                await self.warehouse_repo.insert_properties([ch_record])
                logger.info("property_warehouse_synced", property_id=created.id)
            except Exception as e:
                logger.warning("clickhouse_sync_failed_non_fatal", property_id=created.id, error=str(e))

        await self.cache_service.invalidate()
        return created

    async def update_property(self, property_id: str, property_in: PropertyUpdate) -> Optional[PropertyRead]:
        """Updates property metadata in Redis, re-syncs vectors to Milvus, updates ClickHouse, and invalidates cache."""
        updated = await self.property_repo.update(property_id, property_in)
        if not updated:
            return None

        try:
            passage = self._build_passage(updated)
            vector_arr = await asyncio.to_thread(self.embedder.encode, passage, is_query=False)
            vector = vector_arr.tolist()
            await self.vector_repo.upsert_property_vector(
                property_id=updated.id,
                vector=vector,
                payload={
                    **updated.model_dump(),
                    "text": passage,
                }
            )
        except Exception as e:
            logger.warning("milvus_vector_resync_failed", property_id=property_id, error=str(e))

        if self.warehouse_repo:
            try:
                ch_record = {
                    "id": updated.id,
                    "source": "admin_api",
                    "ingested_from": "property_backend",
                    "title": updated.title,
                    "location": updated.location,
                    "listing_type": updated.listing_type,
                    "property_type": updated.property_type,
                    "price_egp": updated.price_egp,
                    "bedrooms": updated.bedrooms,
                    "bathrooms": updated.bathrooms,
                    "area_sqm": updated.area_sqm,
                    "description": updated.description or "",
                    "url": updated.url or f"https://realestate.internal/properties/{updated.id}",
                }
                await self.warehouse_repo.insert_properties([ch_record])
            except Exception as e:
                logger.warning("clickhouse_resync_failed_non_fatal", property_id=property_id, error=str(e))

        await self.cache_service.invalidate()
        return updated

    async def delete_property(self, property_id: str) -> bool:
        """Deletes property, removes vector from Milvus, and invalidates cache."""
        deleted = await self.property_repo.delete(property_id)
        if not deleted:
            return False

        try:
            await self.vector_repo.delete_property_vector(property_id)
        except Exception as e:
            logger.warning("milvus_vector_delete_failed", property_id=property_id, error=str(e))

        await self.cache_service.invalidate()
        return True
