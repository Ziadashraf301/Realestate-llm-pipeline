"""
Dynamic Metadata & Taxonomy Service.
Discovers and caches live schema metadata, districts, property types, and value boundaries
directly from ClickHouse Analytics Warehouse, with Redis caching.
Injects live database inventory directly into LLM Prompts and Function Calling Tool Schemas.
"""

import copy
import json
from typing import Dict, List, Tuple, Optional, Any
from real_estate.core.redis import get_redis_client
from real_estate.core.logger import logger
from real_estate.repositories.warehouse_repository import ClickHouseWarehouseRepository

CACHE_KEY_METADATA = "cache:live_schema_metadata_enums_v2"
CACHE_TTL_METADATA = 3600  # 1 Hour


class MetadataService:
    """Provides dynamic database-backed vocabulary for intent routing, prompts, and tool schemas."""

    _instance: Optional["MetadataService"] = None
    _warehouse: ClickHouseWarehouseRepository
    _redis: Any

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._warehouse = ClickHouseWarehouseRepository()
            cls._instance._redis = get_redis_client()
        return cls._instance

    async def get_live_metadata(self) -> Dict[str, Any]:
        """
        Retrieves live distinct metadata from Redis cache or ClickHouse warehouse.
        """
        try:
            cached = await self._redis.get(CACHE_KEY_METADATA)
            if cached:
                return json.loads(cached)
        except Exception as e:
            logger.debug("redis_metadata_cache_miss", error=str(e))

        # Query ClickHouse for full metadata and boundaries
        metadata = await self._warehouse.get_distinct_metadata()

        # Supplement with standard Egyptian primary cities if database is empty/warming
        if not metadata.get("locations"):
            metadata["locations"] = [
                "Alexandria, Smouha", "Alexandria, Stanley", "Alexandria, Loran", "Alexandria, Miami",
                "Alexandria, Sidi Gaber", "Alexandria, Kafr Abdo", "Alexandria, Moharam Bek",
                "Cairo, New Cairo", "Cairo, Fifth Settlement", "Cairo, Maadi", "Cairo, Madinaty",
                "Cairo, Rehab", "Cairo, Heliopolis", "Cairo, Nasr City", "Cairo, Shorouk",
                "Giza, Sheikh Zayed", "Giza, 6th of October", "Giza, Mohandessin", "Giza, Dokki"
            ]

        # Extract unique district names from locations
        districts = set()
        for loc in metadata["locations"]:
            parts = [p.strip() for p in loc.split(",") if p.strip()]
            if len(parts) > 1:
                districts.add(parts[1])
            elif parts:
                districts.add(parts[0])
        metadata["districts"] = sorted(list(districts))

        try:
            await self._redis.set(CACHE_KEY_METADATA, json.dumps(metadata), ex=CACHE_TTL_METADATA)
            logger.info(
                "live_schema_metadata_refreshed_from_warehouse",
                location_count=len(metadata.get("locations", [])),
                property_types=metadata.get("property_types", [])
            )
        except Exception:
            pass

        return metadata

    def inject_metadata_into_tool_schema(
        self,
        base_tool: Dict[str, Any],
        metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Dynamically updates the Function Calling tool parameter enums with live database values.
        Ensures the model outputs only valid database values.
        """
        tool = copy.deepcopy(base_tool)
        try:
            props = tool["function"]["parameters"]["properties"]
            # 1. Constrain property_type enum to live database values
            if "property_type" in props and metadata.get("property_types"):
                props["property_type"]["enum"] = metadata["property_types"]

            # 2. Constrain listing_type enum to live database values
            if "listing_type" in props and metadata.get("listing_types"):
                props["listing_type"]["enum"] = metadata["listing_types"]

            # 3. Provide live sample districts in district description
            if "district" in props and metadata.get("districts"):
                sample_dists = ", ".join(metadata["districts"][:12])
                props["district"]["description"] = (
                    f"اسم الحي أو المنطقة المحددة داخل المدينة. نماذج مسجلة في قاعدة البيانات: {sample_dists}"
                )
        except Exception as e:
            logger.warning("inject_metadata_tool_schema_failed", error=str(e))

        return tool

    def inject_metadata_into_prompt(
        self,
        base_prompt: str,
        metadata: Dict[str, Any]
    ) -> str:
        """
        Appends live ClickHouse database inventory context directly into the LLM system prompt.
        """
        prop_types = ", ".join(metadata.get("property_types", []))
        list_types = ", ".join(metadata.get("listing_types", []))
        sample_locs = ", ".join(metadata.get("districts", [])[:20])

        min_p = metadata.get("min_price", 0.0)
        max_p = metadata.get("max_price", 0.0)
        price_info = f"من {min_p:,.0f} إلى {max_p:,.0f} جنيه" if max_p > 0 else "متنوع"

        live_context = (
            "\n\n=======================================================\n"
            "بيانات السوق والمخزون الحي من قاعدة البيانات (Live Database Inventory):\n"
            f"- أنواع العقارات المتاحة: [{prop_types}]\n"
            f"- أنواع المعاملات: [{list_types}]\n"
            f"- عينة من الأحياء والمناطق المسجلة: [{sample_locs}]\n"
            f"- النطاق السعري في قاعدة البيانات: {price_info}\n"
            "توجيه إلزامي: احرص على ربط وتوجيه نية العميل بالقيم الفعلية المتاحة في قاعدة البيانات أعلاه.\n"
            "======================================================="
        )
        return base_prompt + live_context

    EGYPTIAN_HUB_MAPPINGS: List[Tuple[str, str, Optional[str]]] = [
        ("سموحة", "alexandria", "سموحة"),
        ("ستانلي", "alexandria", "ستانلي"),
        ("لوران", "alexandria", "لوران"),
        ("ميامي", "alexandria", "ميامي"),
        ("سيدي جابر", "alexandria", "سيدي جابر"),
        ("كفر عبده", "alexandria", "كفر عبده"),
        ("محرم بك", "alexandria", "محرم بك"),
        ("المنتزه", "alexandria", "المنتزه"),
        ("العجمي", "alexandria", "العجمي"),
        ("الإسكندرية", "alexandria", None),
        ("اسكندرية", "alexandria", None),
        ("التجمع الخامس", "cairo", "التجمع الخامس"),
        ("التجمع", "cairo", "التجمع الخامس"),
        ("المعادي", "cairo", "المعادي"),
        ("مدينتي", "cairo", "مدينتي"),
        ("الرحاب", "cairo", "الرحاب"),
        ("مصر الجديدة", "cairo", "مصر الجديدة"),
        ("مدينة نصر", "cairo", "مدينة نصر"),
        ("الشروق", "cairo", "الشروق"),
        ("القاهرة", "cairo", None),
        ("الشيخ زايد", "giza", "الشيخ زايد"),
        ("زايد", "giza", "الشيخ زايد"),
        ("6 أكتوبر", "giza", "6 أكتوبر"),
        ("أكتوبر", "giza", "6 أكتوبر"),
        ("المهندسين", "giza", "المهندسين"),
        ("الدقي", "giza", "الدقي"),
        ("الهرم", "giza", "الهرم"),
        ("فيصل", "giza", "فيصل"),
        ("الجيزة", "giza", None),
    ]

    def resolve_location_sync(self, query: str) -> Tuple[Optional[str], Optional[str]]:
        """Synchronously resolves Egyptian city and district from known dialect patterns."""
        for kw, city, dist in self.EGYPTIAN_HUB_MAPPINGS:
            if kw in query:
                return city, dist
        return None, None

    async def resolve_location(self, query: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Dynamically resolves (city, district) from the live warehouse vocabulary.
        Matches against actual ingested property locations, falling back to standard hubs.
        """
        meta = await self.get_live_metadata()
        locations = meta.get("locations", [])

        query_norm = query.lower()

        # Check against live warehouse locations first
        for loc in locations:
            parts = [p.strip() for p in loc.split(",") if p.strip()]
            for p in parts:
                if len(p) >= 3 and p.lower() in query_norm:
                    city = parts[0].lower() if parts[0].lower() in ["cairo", "alexandria", "giza"] else None
                    district = parts[1] if len(parts) > 1 else parts[0]
                    return city, district

        # Match against standard hubs
        return self.resolve_location_sync(query)

    async def validate_and_normalize_filters(self, raw_filters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validates extracted filters against the live database inventory and schema.
        Strips unrecognized, empty, or contradictory filter fields so that only
        valid, high-confidence search constraints reach Milvus.
        If no valid constraints exist, returns empty dict to search across all data.
        """
        if not raw_filters:
            return {}

        validated: Dict[str, Any] = {}
        metadata = await self.get_live_metadata()

        # 1. Location & District Validation
        raw_city = raw_filters.get("city") or raw_filters.get("location")
        raw_dist = raw_filters.get("district")
        if raw_city or raw_dist:
            loc_str = f"{raw_city or ''} {raw_dist or ''}".strip()
            city_res, dist_res = await self.resolve_location(loc_str)
            if city_res:
                validated["location"] = city_res
            elif raw_city and str(raw_city).lower() in ["cairo", "alexandria", "giza", "القاهرة", "الإسكندرية", "الجيزة"]:
                validated["location"] = str(raw_city).lower()

            if dist_res:
                validated["district"] = dist_res
            elif raw_dist:
                # Check against live warehouse districts
                live_districts = metadata.get("districts", [])
                for ld in live_districts:
                    if str(raw_dist).lower() in ld.lower() or ld.lower() in str(raw_dist).lower():
                        validated["district"] = ld
                        break

        # 2. Listing Type (Sale / Rent / تمليك / ايجار)
        raw_lt = raw_filters.get("listing_type")
        if raw_lt:
            lt_lower = str(raw_lt).lower()
            if any(w in lt_lower for w in ["rent", "إيجار", "ايجار", "للايجار", "للإيجار"]):
                validated["listing_type"] = "Rent"
            elif any(w in lt_lower for w in ["sale", "بيع", "تمليك", "للبيع"]):
                validated["listing_type"] = "Sale"

        # 3. Property Type
        raw_pt = raw_filters.get("property_type")
        if raw_pt:
            pt_lower = str(raw_pt).lower()
            valid_property_types = {
                "apartment": "Apartment", "villa": "Villa", "duplex": "Duplex",
                "penthouse": "Penthouse", "chalet": "Chalet", "townhouse": "Townhouse",
                "studio": "Studio", "commercial": "Commercial", "building": "Building", "land": "Land",
                "شقة": "Apartment", "فيلا": "Villa", "دوبلكس": "Duplex",
                "بنتهاوس": "Penthouse", "شاليه": "Chalet", "تاون هاوس": "Townhouse",
                "مكتب": "Commercial", "محل": "Commercial", "تجاري": "Commercial",
            }
            # Check direct map or warehouse property types
            matched_pt = valid_property_types.get(pt_lower)
            if not matched_pt:
                for live_pt in metadata.get("property_types", []):
                    if pt_lower in live_pt.lower() or live_pt.lower() in pt_lower:
                        matched_pt = live_pt
                        break
            if matched_pt:
                validated["property_type"] = matched_pt

        # 4. Numerical Fields (Price, Area, Bedrooms, Bathrooms)
        for num_field in ["min_price", "max_price", "min_area_sqm", "max_area_sqm"]:
            val = raw_filters.get(num_field)
            if val is not None:
                try:
                    num_val = float(val)
                    if num_val > 0:
                        validated[num_field] = num_val
                except (ValueError, TypeError):
                    pass

        # Area validation: area must be >= 15 sqm to avoid confusing "3 مليون" with 3 sqm
        if "min_area_sqm" in validated and validated["min_area_sqm"] < 15.0:
            logger.debug("discarding_unrealistically_small_min_area", area=validated["min_area_sqm"])
            validated.pop("min_area_sqm")

        for int_field in ["min_bedrooms", "min_bathrooms", "bedrooms", "bathrooms"]:
            val = raw_filters.get(int_field)
            if val is not None:
                try:
                    int_val = int(val)
                    if int_val > 0:
                        validated[int_field] = int_val
                except (ValueError, TypeError):
                    pass

        return validated


_metadata_service: Optional[MetadataService] = None


def get_metadata_service() -> MetadataService:
    global _metadata_service
    if _metadata_service is None:
        _metadata_service = MetadataService()
    return _metadata_service

