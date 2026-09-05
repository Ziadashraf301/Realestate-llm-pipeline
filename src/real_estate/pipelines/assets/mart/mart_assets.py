"""
Mart Transformation Assets for Dagster (Clean Architecture & ClickHouse).
Dynamically creates mart and analytical summary assets depending on all upstream scraping assets.
"""

import asyncio
from datetime import datetime
from typing import Any, Dict, List
from dagster import asset, OpExecutionContext, RetryPolicy, Output, MetadataValue

from real_estate.core.logger import logger
from real_estate.repositories.warehouse_repository import ClickHouseWarehouseRepository
from real_estate.pipelines.resources.config_resources import MartResource
from .mart_config import MART_CONFIG


def transform_mart_table(
    context: OpExecutionContext,
    mart_resource: MartResource,
    mart_name: str,
    mart_method: str
) -> Output[Dict[str, Any]]:
    """Executes analytical warehouse transformations directly in ClickHouse SQL."""
    context.log.info(f"🔄 Executing ClickHouse Mart: {mart_name} (method={mart_method})...")
    warehouse = ClickHouseWarehouseRepository(
        host=mart_resource.clickhouse_host,
        port=mart_resource.clickhouse_port,
        database=mart_resource.database
    )

    # Physical ClickHouse mart DDL & ETL queries
    MART_TABLE_DDLS: Dict[str, tuple[str, str]] = {
        "validate_property_mart": (
            "property_analytics_mart",
            """
            CREATE OR REPLACE TABLE {db}.property_analytics_mart
            ENGINE = MergeTree
            ORDER BY (location, listing_type, property_type, id) AS
            SELECT 
                id,
                source,
                title,
                location,
                listing_type,
                property_type,
                price_egp,
                price_text,
                currency,
                bedrooms,
                bathrooms,
                area_sqm,
                floor_number,
                round(price_egp / nullIf(area_sqm, 0), 2) AS price_per_sqm,
                address,
                latitude,
                longitude,
                agent_name,
                agent_phone,
                agent_whatsapp,
                agent_type,
                images,
                url,
                scraped_at,
                now() AS mart_synced_at
            FROM {db}.properties FINAL
            WHERE price_egp > 0
            """
        ),
        "create_location_summary": (
            "mart_location_summary",
            """
            CREATE OR REPLACE TABLE {db}.mart_location_summary
            ENGINE = MergeTree
            ORDER BY (location, listing_type) AS
            SELECT 
                location, 
                listing_type, 
                count() AS total_listings,
                round(avg(price_egp), 2) AS avg_price_egp,
                round(median(price_egp), 2) AS median_price_egp,
                round(min(price_egp), 2) AS min_price_egp,
                round(max(price_egp), 2) AS max_price_egp,
                round(avg(area_sqm), 2) AS avg_area_sqm,
                now() AS created_at
            FROM {db}.properties FINAL
            WHERE price_egp > 0
            GROUP BY location, listing_type
            ORDER BY total_listings DESC
            """
        ),
        "create_price_summary": (
            "mart_price_analysis",
            """
            CREATE OR REPLACE TABLE {db}.mart_price_analysis
            ENGINE = MergeTree
            ORDER BY (location, property_type) AS
            SELECT 
                location,
                property_type,
                count() AS total_properties,
                round(quantile(0.25)(price_egp), 2) AS p25_price_egp,
                round(quantile(0.50)(price_egp), 2) AS median_price_egp,
                round(quantile(0.75)(price_egp), 2) AS p75_price_egp,
                round(min(price_egp), 2) AS min_price_egp,
                round(max(price_egp), 2) AS max_price_egp,
                round(avg(price_egp / nullIf(area_sqm, 0)), 2) AS avg_price_per_sqm,
                now() AS created_at
            FROM {db}.properties FINAL
            WHERE price_egp > 0
            GROUP BY location, property_type
            ORDER BY total_properties DESC
            """
        ),
        "create_quality_report": (
            "mart_data_quality",
            """
            CREATE OR REPLACE TABLE {db}.mart_data_quality
            ENGINE = MergeTree
            ORDER BY source AS
            SELECT
                source,
                count() AS total_records,
                countIf(bedrooms IS NOT NULL) AS has_bedrooms,
                countIf(bathrooms IS NOT NULL) AS has_bathrooms,
                countIf(area_sqm IS NOT NULL) AS has_area,
                countIf(latitude IS NOT NULL AND longitude IS NOT NULL) AS has_coordinates,
                countIf(agent_phone IS NOT NULL AND agent_phone != '') AS has_phone,
                countIf(images IS NOT NULL AND length(images) > 0) AS has_images,
                round(countIf(bedrooms IS NOT NULL) / count() * 100, 1) AS bedrooms_pct,
                round(countIf(area_sqm IS NOT NULL) / count() * 100, 1) AS area_pct,
                round(countIf(latitude IS NOT NULL AND longitude IS NOT NULL) / count() * 100, 1) AS coordinates_pct,
                now() AS evaluated_at
            FROM {db}.properties FINAL
            GROUP BY source
            """
        ),
    }

    async def _execute_mart():
        await warehouse.initialize()

        target_table_name, ddl_template = MART_TABLE_DDLS.get(
            mart_method,
            (mart_name, f"CREATE OR REPLACE TABLE {{db}}.{mart_name} ENGINE = MergeTree ORDER BY tuple() AS SELECT * FROM {{db}}.properties FINAL")
        )

        # 1. Physically create and populate the ClickHouse mart table
        ddl_query = ddl_template.format(db=mart_resource.database)
        resp = await warehouse._execute(ddl_query)
        if resp.status_code == 200:
            context.log.info(f"✅ ClickHouse physical table '{mart_resource.database}.{target_table_name}' created and populated successfully.")
        else:
            context.log.error(f"❌ Failed to create mart table '{target_table_name}': {resp.status_code} - {resp.text}")
            raise RuntimeError(f"ClickHouse DDL failed for mart table {target_table_name}: {resp.text}")

        # 2. Count rows in the newly created mart table
        count_query = f"SELECT count() FROM {mart_resource.database}.{target_table_name}"
        count_resp = await warehouse._execute(count_query)
        row_count = 0
        if count_resp.status_code == 200 and count_resp.text.strip().isdigit():
            row_count = int(count_resp.text.strip())

        return target_table_name, row_count

    try:
        table_name, row_count = asyncio.run(_execute_mart())
        context.log.info(f"✅ Mart table '{table_name}' materialized with {row_count} rows in ClickHouse.")

        return Output(
            value={
                "mart_name": mart_name,
                "table_name": table_name,
                "database": mart_resource.database,
                "row_count": row_count,
                "timestamp": datetime.utcnow().isoformat(),
                "status": "success"
            },
            metadata={
                "mart_name": MetadataValue.text(mart_name),
                "clickhouse_table": MetadataValue.text(f"{mart_resource.database}.{table_name}"),
                "row_count": MetadataValue.int(row_count),
                "engine": MetadataValue.text("ClickHouse MergeTree"),
                "status": MetadataValue.text("success")
            }
        )
    except Exception as e:
        context.log.error(f"❌ Error materializing {mart_name}: {str(e)}")
        raise



def create_mart_asset(mart_config: Dict[str, Any]):
    """Factory creating dynamic Dagster mart asset nodes."""
    @asset(
        name=mart_config["asset_name"],
        description=mart_config["description"],
        group_name=mart_config["group_name"],
        deps=mart_config["deps"],
        retry_policy=RetryPolicy(max_retries=2, delay=60)
    )
    def _dynamic_mart(context: OpExecutionContext, mart_resource: MartResource):
        return transform_mart_table(
            context,
            mart_resource,
            mart_config["asset_name"],
            mart_config["mart_method"]
        )

    return _dynamic_mart


def get_all_mart_assets() -> List:
    """Instantiates all mart assets dynamically from MART_CONFIG."""
    assets = []
    for cfg in MART_CONFIG:
        asset_func = create_mart_asset(cfg)
        assets.append(asset_func)
    return assets


mart_assets = get_all_mart_assets()


def get_mart_asset_names() -> List[str]:
    """Returns string asset keys for all dynamically generated mart assets."""
    return [str(asset_def.key.path[-1]) for asset_def in mart_assets]


# Register at module level for Dagster reflection
_asset_map = {asset_def.key.path[-1]: asset_def for asset_def in mart_assets}
for _name, _func in _asset_map.items():
    globals()[_name] = _func