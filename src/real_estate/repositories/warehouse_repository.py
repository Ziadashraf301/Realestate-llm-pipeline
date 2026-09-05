"""
ClickHouse Analytics Warehouse Repository (Replacing BigQuery).
Uses ClickHouse HTTP API on port 8123 (zero external C++ driver bloat).
Employs ReplacingMergeTree with monthly partitioning and compound clustering keys.
"""

from datetime import datetime
from typing import Any, List, Dict, Optional
import httpx

from real_estate.core.settings import settings
from real_estate.core.logger import logger


class ClickHouseWarehouseRepository:
    """Enterprise ClickHouse Data Warehouse for Real Estate Marts and deduplicated properties."""

    def __init__(
        self,
        host: str = settings.CLICKHOUSE_HOST,
        port: int = settings.CLICKHOUSE_PORT,
        database: str = settings.CLICKHOUSE_DB,
        user: str = settings.CLICKHOUSE_USER,
        password: str = settings.CLICKHOUSE_PASSWORD,
        logger: Optional[Any] = None,
    ):
        self.endpoint = f"http://{host}:{port}/"
        self.database = database
        self.user = user
        self.password = password
        self._initialized = False
        self.dagster_logger = logger

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

    async def _execute(self, query: str, data: Optional[str] = None) -> httpx.Response:
        """Executes a query via ClickHouse HTTP endpoint."""
        params = {
            "query": query,
            "database": self.database,
        }
        if self.user:
            params["user"] = self.user
        if self.password:
            params["password"] = self.password

        async with httpx.AsyncClient(timeout=30.0) as client:
            if data:
                return await client.post(self.endpoint, params=params, content=data.encode("utf-8"))
            else:
                return await client.post(self.endpoint, params=params)

    async def initialize(self) -> None:
        """Initializes database and ReplacingMergeTree partitioned table."""
        try:
            # 1. Create Database if not exists
            db_params = {"query": f"CREATE DATABASE IF NOT EXISTS {self.database}"}
            if self.user:
                db_params["user"] = self.user
            if self.password:
                db_params["password"] = self.password
            async with httpx.AsyncClient(timeout=10.0) as client:
                await client.post(self.endpoint, params=db_params)

            # 2. Create Properties ReplacingMergeTree Table
            # Deduplicates strictly by primary key `id`
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.database}.properties (
                id String,
                source LowCardinality(String) DEFAULT 'unknown',
                ingested_from LowCardinality(String) DEFAULT 'web_scraping',
                title String,
                location LowCardinality(String),
                listing_type LowCardinality(String),
                property_type LowCardinality(String),
                price_egp Float64,
                price_text Nullable(String),
                currency LowCardinality(String) DEFAULT 'EGP',
                bedrooms Nullable(Int16),
                bathrooms Nullable(Int16),
                area_sqm Nullable(Float32),
                floor_number Nullable(Int16),
                address Nullable(String),
                latitude Nullable(Float64),
                longitude Nullable(Float64),
                agent_name Nullable(String),
                agent_phone Nullable(String),
                agent_whatsapp Nullable(String),
                agent_type Nullable(String),
                images Array(String),
                description String,
                url String,
                last_updated Nullable(String),
                scraped_at DateTime DEFAULT now(),
                version UInt64 DEFAULT toUnixTimestamp(now())
            )
            ENGINE = ReplacingMergeTree(version)
            PARTITION BY toYYYYMM(scraped_at)
            PRIMARY KEY (id)
            ORDER BY (id);
            """
            resp = await self._execute(create_table_sql)
            if resp.status_code == 200:
                self._initialized = True
                self._log("info","clickhouse_table_initialized_successfully", database=self.database)
            else:
                self._log("warning","clickhouse_init_warning", status=resp.status_code, text=resp.text)
        except Exception as e:
            self._log("warning","clickhouse_unavailable_continuing_in_offline_mode", error=str(e))

    async def optimize_table_deduplicate(self) -> bool:
        """Forces immediate background compaction and deduplication across all parts."""
        try:
            query = f"OPTIMIZE TABLE {self.database}.properties FINAL DEDUPLICATE"
            resp = await self._execute(query)
            return resp.status_code == 200
        except Exception as e:
            self._log("warning", "clickhouse_optimize_failed", error=str(e))
            return False

    async def insert_properties(self, properties: List[Dict[str, Any]], optimize_after: bool = False) -> int:
        """
        Inserts listings via JSONEachRow streaming format.
        ReplacingMergeTree automatically resolves upserts/duplicates by primary key `id`.
        """
        if not properties:
            return 0

        import json
        lines = [json.dumps(p, ensure_ascii=False) for p in properties]
        body = "\n".join(lines)

        query = f"INSERT INTO {self.database}.properties FORMAT JSONEachRow"
        try:
            resp = await self._execute(query, data=body)
            if resp.status_code == 200:
                self._log("info","clickhouse_bulk_insert_success", count=len(properties))
                if optimize_after:
                    await self.optimize_table_deduplicate()
                return len(properties)
            else:
                self._log("error","clickhouse_insert_failed", status=resp.status_code, detail=resp.text)
                return 0
        except Exception as e:
            self._log("error","clickhouse_insert_exception", error=str(e))
            return 0

    async def stream_properties(self, batch_size: int = 500, exclude_source: Optional[str] = None):
        """
        Generator yielding batches of properties for vector builder.
        Ensures constant memory (< 120 MB) regardless of total dataset size.
        Allows filtering out sources like 'admin_api' that are already indexed in real-time.
        """
        offset = 0
        where_clause = f"WHERE source != '{exclude_source}'" if exclude_source else ""
        while True:
            query = f"""
            SELECT id, title, location, listing_type, property_type, price_egp, 
                   bedrooms, bathrooms, area_sqm, floor_number, address, description, url
            FROM {self.database}.properties FINAL
            {where_clause}
            ORDER BY id
            LIMIT {batch_size} OFFSET {offset}
            FORMAT JSONEachRow
            """
            try:
                resp = await self._execute(query)
                if resp.status_code != 200 or not resp.text.strip():
                    break

                import json
                lines = [json.loads(line) for line in resp.text.strip().split("\n") if line.strip()]
                if not lines:
                    break

                yield lines
                offset += len(lines)

                if len(lines) < batch_size:
                    break
            except Exception as e:
                self._log("error","clickhouse_stream_failed", offset=offset, error=str(e))
                break

    async def get_total_count(self) -> int:
        """Returns the total number of deduplicated properties in the warehouse."""
        query = f"SELECT count() FROM {self.database}.properties FINAL FORMAT TabSeparated"
        try:
            resp = await self._execute(query)
            if resp.status_code == 200:
                return int(resp.text.strip())
        except Exception:
            pass
        return 0

    async def get_distinct_metadata(self) -> Dict[str, Any]:
        """
        Queries ClickHouse for distinct live locations, property types, listing types,
        and value boundaries (min/max price, area, bedrooms).
        """
        query = f"""
        SELECT 
            groupUniqArray(location) AS locations,
            groupUniqArray(property_type) AS property_types,
            groupUniqArray(listing_type) AS listing_types,
            min(price_egp) AS min_price,
            max(price_egp) AS max_price,
            min(bedrooms) AS min_bedrooms,
            max(bedrooms) AS max_bedrooms,
            min(area_sqm) AS min_area_sqm,
            max(area_sqm) AS max_area_sqm
        FROM {self.database}.properties FINAL
        FORMAT JSON
        """
        try:
            resp = await self._execute(query)
            if resp.status_code == 200:
                data = resp.json()
                rows = data.get("data", [])
                if rows:
                    r = rows[0]
                    locations = sorted([str(x) for x in r.get("locations", []) if x])
                    prop_types = sorted([str(x) for x in r.get("property_types", []) if x])
                    list_types = sorted([str(x) for x in r.get("listing_types", []) if x])
                    logger.info(
                        "clickhouse_distinct_metadata_retrieved",
                        location_count=len(locations),
                        property_types_count=len(prop_types),
                        listing_types=list_types,
                        price_range=(float(r.get("min_price") or 0.0), float(r.get("max_price") or 0.0)),
                    )
                    return {
                        "locations": locations,
                        "property_types": prop_types or ["Apartment", "Villa", "Duplex", "Penthouse", "Chalet", "Townhouse", "Commercial"],
                        "listing_types": list_types or ["Sale", "Rent"],
                        "min_price": float(r.get("min_price") or 0.0),
                        "max_price": float(r.get("max_price") or 0.0),
                        "min_bedrooms": int(r.get("min_bedrooms") or 0),
                        "max_bedrooms": int(r.get("max_bedrooms") or 0),
                        "min_area_sqm": float(r.get("min_area_sqm") or 0.0),
                        "max_area_sqm": float(r.get("max_area_sqm") or 0.0)
                    }
        except Exception as e:
            logger.warning("clickhouse_distinct_metadata_failed", error=str(e))

        return {
            "locations": [],
            "property_types": ["Apartment", "Villa", "Duplex", "Penthouse", "Chalet", "Townhouse", "Commercial"],
            "listing_types": ["Sale", "Rent"],
            "min_price": 0.0,
            "max_price": 100000000.0,
            "min_bedrooms": 1,
            "max_bedrooms": 10,
            "min_area_sqm": 20.0,
            "max_area_sqm": 2000.0
        }


# Alias for backward compatibility
ClickHouseWarehouse = ClickHouseWarehouseRepository
