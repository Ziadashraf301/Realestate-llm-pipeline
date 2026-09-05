"""
Milvus Vector Repository (MilvusClient 2.5 — Native Hybrid Search).

Uses Milvus 2.5 native hybrid search:
  - Dense path  : FLOAT_VECTOR (384-dim, HNSW, COSINE) via ONNX embedder
  - Sparse path : SPARSE_FLOAT_VECTOR auto-generated from 'text' by BM25 Function
  - Fusion      : Native RRFRanker(k=60) — zero manual Python RRF loop
  - Filter      : Single expr applied uniformly to BOTH paths via Milvus engine

Collection schema is self-initializing via initialize_collection().
Existing collections with old schema (no sparse_vector field) must be dropped and recreated.
"""

import hashlib
import json
import time
from typing import Any, Optional

from pymilvus import (
    MilvusClient,
    DataType,
    Function,
    FunctionType,
    AnnSearchRequest,
    RRFRanker,
)

from real_estate.core.settings import settings
from real_estate.core.logger import logger
from real_estate.repositories.base import BaseVectorRepository


def _hash_filters(filters: dict[str, Any] | None) -> str:
    """Computes a deterministic SHA-256 hash across all active search filters for semantic cache isolation."""
    if not filters:
        return "none"
    serialized = json.dumps(filters, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


# Normalization maps matching exact Milvus collection vocabulary
_LISTING_TYPE_MAP: dict[str, list[str]] = {
    "sale": ["تمليك", "Sale", "sale", "for-sale", "for_sale", "عقارات-للبيع", "للبيع"],
    "rent": ["ايجار", "إيجار", "Rent", "rent", "for-rent", "for_rent", "عقارات-للايجار", "للإيجار", "للايجار"],
}

_PROPERTY_TYPE_MAP: dict[str, list[str]] = {
    "apartment": ["شقة", "شقة مفروشة", "Apartment", "apartment", "شقق", "شقق للبيع", "شقق للايجار"],
    "villa": ["فيلا", "فيلات", "فلل", "Villa", "villa"],
    "duplex": ["دوبلكس", "Duplex", "duplex"],
    "penthouse": ["بنتهاوس", "Penthouse", "penthouse"],
    "chalet": ["شاليه", "شاليهات", "Chalet", "chalet"],
    "townhouse": ["تاون هاوس", "Townhouse", "townhouse", "Twin House", "twin-house", "توين هاوس"],
    "studio": ["استوديو", "ستوديو", "Studio", "studio"],
    "commercial": ["مكتب", "محل", "عيادة", "Commercial", "commercial", "تجاري", "إداري", "مبنى تجاري"],
    "building": ["عمارة", "مبنى", "Building", "building"],
    "land": ["أرض", "ارض", "Land", "land"],
}

_CITY_MAP: dict[str, list[str]] = {
    "alexandria": ["alexandria", "Alexandria", "إسكندرية", "اسكندرية", "الإسكندرية", "الاسكندرية"],
    "cairo": ["cairo", "Cairo", "القاهرة", "قاهرة"],
    "giza": ["giza", "Giza", "الجيزة", "جيزة"],
}


def _build_filter_expr(filters: dict[str, Any] | None) -> str | None:
    """
    Builds a Milvus boolean expression from the normalized filter dict.
    Single source of truth — applied uniformly to BOTH dense and sparse search paths.
    Matches exact Milvus column values (cairo/alexandria/giza, تمليك/ايجار, شقة/فيلا/مكتب).
    """
    if not filters:
        return None

    conditions: list[str] = []

    # 1. Governorate / City Filter (Milvus location field stores 'cairo', 'alexandria', 'giza')
    raw_city = str(filters.get("location") or filters.get("city") or "").lower().strip()
    if raw_city:
        matched_city = None
        for k, vlist in _CITY_MAP.items():
            if raw_city == k or any(raw_city == v.lower() for v in vlist):
                matched_city = k
                break
        if matched_city:
            allowed_cities = _CITY_MAP.get(matched_city, [matched_city])
            allowed_city_str = ", ".join(f"'{v}'" for v in allowed_cities)
            conditions.append(f"location in [{allowed_city_str}]")

    # 2. Transaction Type (listing_type: 'تمليك' / 'ايجار')
    raw_lt = str(filters.get("listing_type") or "").strip()
    if raw_lt:
        lt_key = "rent" if any(w in raw_lt.lower() for w in ["rent", "ايجار", "إيجار"]) else "sale"
        allowed = _LISTING_TYPE_MAP.get(lt_key, [raw_lt])
        allowed_str = ", ".join(f"'{v}'" for v in allowed)
        conditions.append(f"listing_type in [{allowed_str}]")

    # 3. Property Type ('شقة', 'فيلا', 'مكتب', 'شاليه', etc.)
    raw_pt = str(filters.get("property_type") or "").strip()
    if raw_pt:
        pt_key = raw_pt.lower()
        allowed_types = None
        for k, vlist in _PROPERTY_TYPE_MAP.items():
            if any(pt_key == v.lower() for v in vlist):
                allowed_types = vlist
                break
        if not allowed_types:
            allowed_types = [raw_pt, raw_pt.lower(), raw_pt.title()]
        allowed_pt_str = ", ".join(f"'{v}'" for v in allowed_types)
        conditions.append(f"property_type in [{allowed_pt_str}]")

    # 5. Price range (only positive non-zero amounts)
    min_p = float(filters["min_price"]) if filters.get("min_price") is not None and float(filters["min_price"]) > 0 else None
    max_p = float(filters["max_price"]) if filters.get("max_price") is not None and float(filters["max_price"]) > 0 else None
    if min_p is not None and max_p is not None and min_p > max_p:
        min_p, max_p = max_p, min_p
    if min_p is not None:
        conditions.append(f"price_egp >= {min_p}")
    if max_p is not None:
        conditions.append(f"price_egp <= {max_p}")

    # 6. Bedroom / Bathroom counts (only positive non-zero integers)
    min_beds = filters.get("min_bedrooms") if filters.get("min_bedrooms") is not None else filters.get("bedrooms")
    if min_beds is not None and int(min_beds) > 0:
        conditions.append(f"bedrooms >= {int(min_beds)}")

    min_baths = filters.get("min_bathrooms") if filters.get("min_bathrooms") is not None else filters.get("bathrooms")
    if min_baths is not None and int(min_baths) > 0:
        conditions.append(f"bathrooms >= {int(min_baths)}")

    # 7. Area range (sqm) (guard against zero or inverted min > max hallucinated constraints)
    min_a = float(filters["min_area_sqm"]) if filters.get("min_area_sqm") is not None and float(filters["min_area_sqm"]) > 0 else None
    max_a = float(filters["max_area_sqm"]) if filters.get("max_area_sqm") is not None and float(filters["max_area_sqm"]) > 0 else None
    if min_a is not None and max_a is not None and min_a > max_a:
        max_a = None  # drop impossible max filter
    if min_a is not None:
        conditions.append(f"area_sqm >= {min_a}")
    if max_a is not None:
        conditions.append(f"area_sqm <= {max_a}")

    expr = " and ".join(conditions) if conditions else None
    if expr:
        logger.info("milvus_filter_expression_built", filter_expr=expr, raw_filters=filters)
    return expr


class MilvusVectorRepository(BaseVectorRepository):
    """
    Milvus 2.5 vector repository using native hybrid search.
    Dense (HNSW COSINE) + Sparse (BM25 auto-index) fused by RRFRanker(k=60).
    """

    _client: MilvusClient | None = None

    # Output fields returned by hybrid_search for property listings
    _PROPERTY_OUTPUT_FIELDS = [
        "id", "title", "location", "price_egp", "listing_type",
        "property_type", "bedrooms", "bathrooms", "area_sqm", "text", "url",
    ]

    def __init__(self, logger: Optional[Any] = None):
        self.dagster_logger = logger
        
        if MilvusVectorRepository._client is None:
            try:
                MilvusVectorRepository._client = MilvusClient(
                    uri=f"http://{settings.MILVUS_HOST}:{settings.MILVUS_PORT}"
                )
                self._log("info", "milvus_client_connected", host=settings.MILVUS_HOST, port=settings.MILVUS_PORT)
            except Exception as e:
                self._log("warning", "milvus_client_connection_failed", error=str(e))

        self.collection_name = settings.MILVUS_COLLECTION_NAME
        self.semantic_cache_name = settings.MILVUS_SEMANTIC_CACHE_COLLECTION

    def _log(self, level: str, msg: str, **kwargs) -> None:
        """Emits to Dagster context.log (if provided) and to standard structlog."""
        extra_str = " | ".join(f"{k}={v}" for k, v in kwargs.items())
        display_msg = f"{msg} ({extra_str})" if extra_str else msg
        if self.dagster_logger:
            try:
                fn = getattr(self.dagster_logger, level, None) or self.dagster_logger.info
                fn(display_msg)
            except Exception:
                pass
        struct_func = getattr(logger, level, None) or logger.info
        struct_func(msg, **kwargs)

    @property
    def client(self) -> MilvusClient | None:
        return MilvusVectorRepository._client

    # -------------------------------------------------------------------------
    # Schema Initialization (Self-Healing Collection)
    # -------------------------------------------------------------------------
    def initialize_collection(self) -> None:
        """
        Creates the property listings collection with Milvus 2.5 hybrid schema:
          - vector        : FLOAT_VECTOR 384-dim (HNSW, COSINE) — dense path
          - text          : VARCHAR with enable_analyzer — BM25 input field
          - sparse_vector : SPARSE_FLOAT_VECTOR auto-populated by BM25 Function — sparse path
          - Scalar fields : title, location, price_egp, listing_type, property_type,
                            bedrooms, bathrooms, area_sqm, url

        The BM25 Function maps 'text' → 'sparse_vector' at insert/upsert time automatically.
        No manual sparse embedding is required in the ingestion pipeline.
        Safe no-op if collection already exists.
        """
        if self.client is None:
            logger.warning("milvus_initialize_skipped_no_client")
            return

        try:
            # Ensure semantic query cache collection exists
            self._initialize_semantic_cache_collection()

            if self.client.has_collection(self.collection_name):
                self._log("info", "milvus_collection_exists", collection=self.collection_name)
                self.client.load_collection(self.collection_name)
                return

            self._log("info", "milvus_creating_hybrid_collection", collection=self.collection_name)

            schema = MilvusClient.create_schema(auto_id=False, enable_dynamic_field=False)

            # Primary key
            schema.add_field("id", DataType.VARCHAR, is_primary=True, max_length=128)

            # Dense vector (384-dim multilingual-e5-small ONNX)
            schema.add_field("vector", DataType.FLOAT_VECTOR, dim=384)

            # BM25 input text — Milvus analyzes this to populate sparse_vector automatically
            schema.add_field(
                "text",
                DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params={"tokenizer": "standard"},
            )

            # Sparse vector — auto-populated by BM25 Function (no manual embedding at ingestion)
            schema.add_field("sparse_vector", DataType.SPARSE_FLOAT_VECTOR)

            # Scalar metadata fields
            schema.add_field("title", DataType.VARCHAR, max_length=512)
            schema.add_field("location", DataType.VARCHAR, max_length=256)
            schema.add_field("price_egp", DataType.FLOAT)
            schema.add_field("listing_type", DataType.VARCHAR, max_length=64)
            schema.add_field("property_type", DataType.VARCHAR, max_length=64)
            schema.add_field("bedrooms", DataType.INT16, nullable=True)
            schema.add_field("bathrooms", DataType.INT16, nullable=True)
            schema.add_field("area_sqm", DataType.FLOAT, nullable=True)
            schema.add_field("url", DataType.VARCHAR, max_length=1024)

            # BM25 Function: Milvus converts text → sparse_vector at write time
            bm25_fn = Function(
                name="bm25_text_to_sparse",
                function_type=FunctionType.BM25,
                input_field_names=["text"],
                output_field_names=["sparse_vector"],
            )
            schema.add_function(bm25_fn)

            # Index parameters
            index_params = MilvusClient.prepare_index_params()

            # Dense HNSW index (COSINE similarity)
            index_params.add_index(
                field_name="vector",
                index_type="HNSW",
                metric_type="COSINE",
                params={"M": 16, "efConstruction": 256},
            )

            # Sparse inverted index required for BM25 Function output field
            index_params.add_index(
                field_name="sparse_vector",
                index_type="SPARSE_INVERTED_INDEX",
                metric_type="BM25",
            )

            self.client.create_collection(
                collection_name=self.collection_name,
                schema=schema,
                index_params=index_params,
            )
            self.client.load_collection(self.collection_name)
            self._log(
                "info",
                "milvus_hybrid_collection_created_and_loaded",
                collection=self.collection_name,
                dense_dim=384,
                sparse_metric="BM25",
            )
        except Exception as e:
            self._log("warning", "milvus_collection_init_exception", error=str(e))

    def _initialize_semantic_cache_collection(self) -> None:
        """Initializes the semantic cache collection if not present."""
        if self.client is None:
            return
        try:
            if self.client.has_collection(self.semantic_cache_name):
                return

            logger.info("milvus_creating_semantic_cache_collection", collection=self.semantic_cache_name)
            cache_schema = MilvusClient.create_schema(auto_id=True, enable_dynamic_field=False)
            cache_schema.add_field("id", DataType.INT64, is_primary=True)
            cache_schema.add_field("query_vector", DataType.FLOAT_VECTOR, dim=384)
            cache_schema.add_field("query_text", DataType.VARCHAR, max_length=2048)
            cache_schema.add_field("response_json", DataType.VARCHAR, max_length=65535)
            cache_schema.add_field("expires_at", DataType.INT64)
            cache_schema.add_field("filter_hash", DataType.VARCHAR, max_length=64)
            cache_schema.add_field("location", DataType.VARCHAR, max_length=256)
            cache_schema.add_field("listing_type", DataType.VARCHAR, max_length=64)

            cache_index_params = MilvusClient.prepare_index_params()
            cache_index_params.add_index(
                field_name="query_vector",
                index_type="HNSW",
                metric_type="COSINE",
                params={"M": 16, "efConstruction": 256},
            )
            self.client.create_collection(
                collection_name=self.semantic_cache_name,
                schema=cache_schema,
                index_params=cache_index_params,
            )
            logger.info("milvus_semantic_cache_collection_created", collection=self.semantic_cache_name)
        except Exception as e:
            logger.warning("milvus_semantic_cache_init_exception", error=str(e))

    # -------------------------------------------------------------------------
    # Native Hybrid Search (Dense HNSW + BM25 Sparse + RRFRanker)
    # -------------------------------------------------------------------------
    async def hybrid_search(
        self,
        query_vector: list[float],
        query_text: str,
        top_k: int = 20,
        filters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Executes Milvus 2.5 native hybrid search.

        Dense path  : AnnSearchRequest on 'vector' field (HNSW, COSINE)
        Sparse path : AnnSearchRequest on 'sparse_vector' field (BM25, raw text input)
        Fusion      : RRFRanker(k=60) — same formula as former manual Python loop, now native
        Filter      : Single _build_filter_expr applied uniformly to BOTH paths

        This replaces: ArabicBM25Retriever + manual RRF Python loop.
        """
        if self.client is None:
            logger.warning("milvus_hybrid_search_skipped_no_client")
            return []

        expr = _build_filter_expr(filters)

        # Dense ANN request (HNSW COSINE on 384-dim dense vector)
        dense_req = AnnSearchRequest(
            data=[query_vector],
            anns_field="vector",
            param={"metric_type": "COSINE", "params": {"ef": 64}},
            limit=top_k,
            expr=expr,
        )

        # Sparse BM25 request — Milvus tokenizes raw query_text using collection's BM25 analyzer
        sparse_req = AnnSearchRequest(
            data=[query_text],
            anns_field="sparse_vector",
            param={"metric_type": "BM25"},
            limit=top_k,
            expr=expr,
        )

        t0 = time.perf_counter()
        try:
            results = self.client.hybrid_search(
                collection_name=self.collection_name,
                reqs=[dense_req, sparse_req],
                ranker=RRFRanker(k=60),
                limit=top_k,
                output_fields=self._PROPERTY_OUTPUT_FIELDS,
            )
        except Exception as e:
            logger.warning("milvus_hybrid_search_failed", error=str(e), filter_expr=expr or "none")
            return []

        duration_ms = (time.perf_counter() - t0) * 1000
        hits: list[dict[str, Any]] = []
        # Milvus returns a list of result lists/HybridHits (one per query request)
        raw_hit_list = []
        if results:
            first_item = results[0]
            if isinstance(first_item, (list, tuple)) or (hasattr(first_item, "__iter__") and not isinstance(first_item, dict)):
                raw_hit_list = list(first_item)
            else:
                raw_hit_list = results

        for hit in raw_hit_list:
            entity: dict[str, Any] = {}
            if isinstance(hit, dict):
                entity = dict(hit.get("entity", {})) if "entity" in hit else {str(k): v for k, v in hit.items() if k not in ("id", "distance", "similarity")}
                entity["id"] = hit.get("id")
                entity["similarity"] = hit.get("distance", hit.get("similarity"))
            else:
                # Milvus Hit object inside HybridHits
                hit_entity = getattr(hit, "entity", None)
                hit_fields = getattr(hit, "fields", None)
                if isinstance(hit_entity, dict):
                    entity = dict(hit_entity)
                elif isinstance(hit_fields, dict):
                    entity = dict(hit_fields)
                elif hasattr(hit, "to_dict") and callable(getattr(hit, "to_dict")):
                    d = hit.to_dict()
                    entity = dict(d.get("entity", {})) if isinstance(d, dict) and "entity" in d else (dict(d) if isinstance(d, dict) else {})
                else:
                    for field in self._PROPERTY_OUTPUT_FIELDS:
                        if hasattr(hit, field):
                            entity[field] = getattr(hit, field)

                entity["id"] = getattr(hit, "id", entity.get("id"))
                entity["similarity"] = getattr(hit, "distance", getattr(hit, "score", entity.get("similarity")))
            hits.append(entity)

        logger.info(
            "milvus_hybrid_search_complete",
            query_preview=query_text[:50],
            top_k=top_k,
            hits=len(hits),
            filter_expr=expr or "none",
            latency_ms=round(duration_ms, 2),
            top_hit_ids=[h.get("id") for h in hits[:3]],
        )
        return hits


    # -------------------------------------------------------------------------
    # Semantic Cache Collection
    # -------------------------------------------------------------------------
    async def search_semantic_cache(
        self,
        query_vector: list[float],
        filters: dict[str, Any] | None = None,
        threshold: float = 0.96,
    ) -> dict[str, Any] | None:
        """
        Retrieves matching pre-computed response if:
        1. Semantic Cosine similarity >= threshold (default 0.96)
        2. Cache TTL has NOT expired (expires_at > current_timestamp)
        3. All search filters match exactly via deterministic filter_hash (location, price, rooms, type, etc.)
        """
        if self.client is None:
            return None

        try:
            if not self.client.has_collection(self.semantic_cache_name):
                self._initialize_semantic_cache_collection()
                return None
        except Exception:
            return None

        now_ts = int(time.time())
        f_hash = _hash_filters(filters)
        expr = f"expires_at > {now_ts} and filter_hash == '{f_hash}'"

        try:
            results = self.client.search(
                collection_name=self.semantic_cache_name,
                data=[query_vector],
                anns_field="query_vector",
                search_params={"metric_type": "COSINE", "params": {"ef": 64}},
                limit=1,
                filter=expr,
                output_fields=["response_json", "expires_at", "filter_hash"],
            )
            if results and len(results[0]) > 0:
                top = results[0][0]
                if isinstance(top, dict):
                    distance = top.get("distance", 0)
                    entity = top.get("entity", {})
                else:
                    distance = getattr(top, "distance", getattr(top, "score", 0))
                    entity = getattr(top, "entity", {}) if isinstance(getattr(top, "entity", None), dict) else getattr(top, "fields", {})

                if distance >= threshold:
                    resp_json = entity.get("response_json") if isinstance(entity, dict) else getattr(entity, "response_json", None)
                    return {
                        "similarity": distance,
                        "data": resp_json,
                    }
        except Exception as e:
            logger.warning("semantic_cache_search_failed", error=str(e))
        return None

    async def insert_semantic_cache(
        self,
        query_vector: list[float],
        query_text: str,
        response_json: str,
        expires_at: int,
        filters: dict[str, Any] | None = None,
    ) -> None:
        """Stores query embedding, response payload, expiration timestamp, and filter hash for 100% isolation."""
        if self.client is None:
            return
        try:
            if not self.client.has_collection(self.semantic_cache_name):
                self._initialize_semantic_cache_collection()

            f_hash = _hash_filters(filters)
            self.client.insert(
                collection_name=self.semantic_cache_name,
                data=[{
                    "query_vector": query_vector,
                    "query_text": query_text,
                    "response_json": response_json,
                    "expires_at": expires_at,
                    "filter_hash": f_hash,
                    "location": str((filters or {}).get("location", "")),
                    "listing_type": str((filters or {}).get("listing_type", "")),
                }],
            )
        except Exception as e:
            logger.warning("semantic_cache_insert_failed", error=str(e))

    # -------------------------------------------------------------------------
    # Property Vector Upsert / Delete
    # -------------------------------------------------------------------------
    async def upsert_property_vector(
        self,
        property_id: str,
        vector: list[float],
        payload: dict[str, Any],
    ) -> None:
        """
        Upserts property vector with full payload into Milvus.
        'sparse_vector' is populated automatically by the BM25 Function from 'text'.
        """
        if self.client is None:
            return
        try:
            row: dict[str, Any] = {
                "id": property_id,
                "vector": vector,
                "text": str(payload.get("text", "") or payload.get("description", "")),
                "title": str(payload.get("title", "")),
                "location": str(payload.get("location", "")),
                "price_egp": float(payload.get("price_egp") or 0.0),
                "listing_type": str(payload.get("listing_type", "")),
                "property_type": str(payload.get("property_type", "")),
                "bedrooms": int(payload.get("bedrooms")) if payload.get("bedrooms") is not None else 0,
                "bathrooms": int(payload.get("bathrooms")) if payload.get("bathrooms") is not None else 0,
                "area_sqm": float(payload.get("area_sqm")) if payload.get("area_sqm") is not None else 0.0,
                "url": str(payload.get("url", "")),
            }

            self.client.upsert(
                collection_name=self.collection_name,
                data=[row],
            )
        except Exception as e:
            logger.warning("vector_upsert_failed", prop_id=property_id, error=str(e))

    async def upsert_property_vectors_batch(
        self,
        items: list[dict[str, Any]],
    ) -> None:
        """
        Bulk upserts a list of property records with vectors and payloads into Milvus in a single RPC.
        """
        if self.client is None or not items:
            return


        formatted_data = []
        for item in items:
            prop_id = item["id"]
            vector = item["vector"]
            payload = item.get("payload", {})
            row_dict: dict[str, Any] = {
                "id": str(prop_id),
                "vector": vector,
                "text": str(payload.get("text", "") or payload.get("description", "")),
                "title": str(payload.get("title", "")),
                "location": str(payload.get("location", "")),
                "price_egp": float(payload.get("price_egp") or 0.0),
                "listing_type": str(payload.get("listing_type", "")),
                "property_type": str(payload.get("property_type", "")),
                "bedrooms": int(payload.get("bedrooms")) if payload.get("bedrooms") is not None else 0,
                "bathrooms": int(payload.get("bathrooms")) if payload.get("bathrooms") is not None else 0,
                "area_sqm": float(payload.get("area_sqm")) if payload.get("area_sqm") is not None else 0.0,
                "url": str(payload.get("url", "")),
            }
            formatted_data.append(row_dict)

        try:
            res = self.client.upsert(
                collection_name=self.collection_name,
                data=formatted_data,
            )
            logger.info("vector_batch_upsert_success", count=len(formatted_data), milvus_result=str(res))
        except Exception as e:
            logger.warning("vector_batch_upsert_failed", count=len(formatted_data), error=str(e))

    async def delete_property_vector(self, property_id: str) -> None:
        if self.client is None:
            return
        try:
            self.client.delete(
                collection_name=self.collection_name,
                filter=f"id == '{property_id}'",
            )
        except Exception as e:
            logger.warning("vector_delete_failed", prop_id=property_id, error=str(e))
