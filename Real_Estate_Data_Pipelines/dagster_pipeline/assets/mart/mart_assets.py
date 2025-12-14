"""Mart transformation assets for real estate pipeline - Fully dynamic generation"""
from datetime import datetime
from typing import Dict, Any, List
from dagster import asset, OpExecutionContext, RetryPolicy
from src.config import config
from ...resources.config_resources import MartResource
from .mart_config import MART_CONFIG


def transform_mart_table(
    context: OpExecutionContext,
    mart_resource: MartResource,
    mart_name: str,
    mart_method: str,
    is_main_mart: bool = False
) -> Dict[str, Any]:
    """
    Generic transformation function for any mart table
    
    Args:
        context: Dagster execution context
        mart_resource: Mart resource configuration
        mart_name: Name of the mart (for logging)
        mart_method: Method name to call on PropertyMartBuilder
        is_main_mart: True if this is the main property_mart table
    """
    try:
        context.log.info(f"🔄 Starting {mart_name} transformation...")
        
        from src.etl import PropertyMartBuilder
        from src.databases import Big_Query_Database
        
        # Initialize database
        db = Big_Query_Database(
            project_id=mart_resource.project_id,
            raw_dataset_id=mart_resource.raw_dataset_id,
            raw_table_id=mart_resource.raw_table_id,
            mart_dataset_id=mart_resource.mart_dataset_id,
            mart_table_id=mart_resource.mart_table_id,
            log_dir=mart_resource.log_dir
        )
        db.connect()
        
        # Initialize mart builder
        mart_builder = PropertyMartBuilder(
            log_dir=mart_resource.log_dir,
            db=db
        )
        
        # Call the appropriate method
        method = getattr(mart_builder, mart_method)
        result = method()
        
       # Build return dictionary based on mart type
        from dagster import Output, MetadataValue
        
        if is_main_mart:
            context.log.info(f"✅ {mart_name} created with total {result:,} rows")
            return Output(
                value={
                    "row_count": result,
                    "table_name": mart_name,
                    "timestamp": datetime.now().isoformat(),
                    "status": "success"
                },
                metadata={
                    "row_count": MetadataValue.int(result),
                    "table_name": MetadataValue.text(mart_name),
                    "status": MetadataValue.text("success")
                }
            )
        else:
            context.log.info(f"✅ {mart_name} snapshot created successfully")
            return Output(
                value={
                    "row_count": 1,
                    "table_name": mart_name,
                    "snapshot_date": datetime.now().date().isoformat(),
                    "timestamp": datetime.now().isoformat(),
                    "status": "success"
                },
                metadata={
                    "row_count": MetadataValue.int(1),
                    "table_name": MetadataValue.text(mart_name),
                    "status": MetadataValue.text("success")
                }
            )
        
    except Exception as e:
        context.log.error(f"❌ Error creating {mart_name}: {str(e)}")
        import traceback
        context.log.error(traceback.format_exc())
        
        return {
            "row_count": 0,  # ← Consistent field name
            "table_name": mart_name,  # ← Added
            "timestamp": datetime.now().isoformat(),
            "status": "failed",
            "error": str(e)
        }


def create_mart_asset(
    asset_name: str,
    description: str,
    group_name: str,
    deps: List[str],
    mart_method: str,
    is_main_mart: bool = False
):
    """
    Factory function to dynamically create mart assets
    
    Args:
        asset_name: Name of the asset
        description: Asset description
        group_name: Dagster group name
        deps: List of dependency asset names
        mart_method: Method name to call on PropertyMartBuilder
        is_main_mart: Whether this is the main property mart
    
    Returns:
        Dagster asset function
    """
    
    @asset(
        name=asset_name,
        description=description,
        group_name=group_name,
        deps=deps,
        retry_policy=RetryPolicy(max_retries=2, delay=300)
    )
    def mart_asset(context: OpExecutionContext, mart_resource: MartResource):
        return transform_mart_table(
            context,
            mart_resource,
            asset_name,  # ← Pass asset_name directly
            mart_method,
            is_main_mart
        )
    
    return mart_asset


def get_all_mart_assets() -> List:
    """
    Dynamically generate all mart assets from config
    
    Returns:
        List of all mart asset functions
    """
    assets = []
    for mart_config in MART_CONFIG:
        asset_func = create_mart_asset(
            asset_name=mart_config["asset_name"],
            description=mart_config["description"],
            group_name=mart_config["group_name"],
            deps=mart_config["deps"],
            mart_method=mart_config["mart_method"],
            is_main_mart=mart_config.get("is_main_mart", False)
        )
        assets.append(asset_func)
    
    return assets


def get_mart_asset_names() -> List[str]:
    """
    Get list of all mart asset names for use in job definitions
    
    Returns:
        List of asset names as strings
    """
    return [cfg["asset_name"] for cfg in MART_CONFIG]


# Generate all assets dynamically
mart_assets = get_all_mart_assets()

# Create module-level variables dynamically
_asset_map = {asset.key.path[-1]: asset for asset in mart_assets}
for asset_name, asset_func in _asset_map.items():
    globals()[asset_name] = asset_func