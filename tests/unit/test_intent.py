"""
Unit Tests for Colloquial Egyptian Arabic Intent Parsing & Milvus Routing (Milestone 3).
Validates schema alignment, function-calling tool generation, few-shot prompts, and filter conversions.
"""

import pytest
from real_estate.services.intent_service import IntentService
from real_estate.schemas.intent import get_intent_function_calling_tool, ExtractedQueryIntent
from real_estate.core.prompt_registry import MLflowPromptRegistry, FEW_SHOT_INTENT_EXAMPLES


def test_intent_parsing_alexandria():
    service = IntentService()
    intent = service.parse_intent("عايز شقة للبيع في سموحة 3 غرف بأقل من 3 مليون جنيه")

    assert intent.city == "alexandria"
    assert intent.district == "سموحة"
    assert intent.listing_type == "Sale"
    assert intent.property_type == "Apartment"
    assert intent.bedrooms == 3
    assert intent.max_price == 3000000.0


def test_intent_parsing_cairo_rent():
    service = IntentService()
    intent = service.parse_intent("دوبلكس للايجار في التجمع الخامس ميزانية 45 ألف")

    assert intent.city == "cairo"
    assert "التجمع" in (intent.district or "")
    assert intent.listing_type == "Rent"
    assert intent.property_type == "Duplex"
    assert intent.max_price == 45000.0


def test_intent_parsing_giza_villa():
    service = IntentService()
    intent = service.parse_intent("فيلا فاخرة للبيع في الشيخ زايد 4 غرف مساحة 350 متر")

    assert intent.city == "giza"
    assert intent.district == "الشيخ زايد"
    assert intent.listing_type == "Sale"
    assert intent.property_type == "Villa"
    assert intent.bedrooms == 4
    assert intent.min_area_sqm == 350.0


def test_function_calling_tool_schema():
    tool = get_intent_function_calling_tool()
    assert tool["type"] == "function"
    assert tool["function"]["name"] == "extract_real_estate_intent"
    properties = tool["function"]["parameters"]["properties"]
    assert "city" in properties
    assert "district" in properties
    assert "listing_type" in properties
    assert "property_type" in properties
    assert "min_price" in properties
    assert "max_price" in properties


def test_mlflow_prompt_registry_few_shots():
    prompt = MLflowPromptRegistry.get_intent_prompt()
    assert "Milvus Metadata Schema" in prompt
    assert len(FEW_SHOT_INTENT_EXAMPLES) >= 3
    assert "سموحة" in prompt
    assert "التجمع الخامس" in prompt
    assert "الشيخ زايد" in prompt


def test_intent_to_filter_dict():
    intent = ExtractedQueryIntent(
        city="cairo",
        district="المعادي",
        listing_type="Rent",
        property_type="Apartment",
        max_price=30000.0,
        min_bedrooms=2,
        cleaned_semantic_query="شقة مفروشة المعادي"
    )
    filters = intent.to_filter_dict()
    assert filters["city"] == "cairo"
    assert filters["district"] == "المعادي"
    assert filters["listing_type"] == "Rent"
    assert filters["property_type"] == "Apartment"
    assert filters["max_price"] == 30000.0
    assert filters["min_bedrooms"] == 2
    assert "cleaned_semantic_query" not in filters


def test_advisor_prompt_registry():
    sys_inst = MLflowPromptRegistry.get_advisor_system_instruction()
    assert "مستشار عقاري" in sys_inst
    assert "Zero Hallucination" in sys_inst

    user_p = MLflowPromptRegistry.build_advisor_user_prompt(
        query="شقة في سموحة",
        context_text="العقار 1: شقة راقية بسعر 2 مليون"
    )
    assert "شقة في سموحة" in user_p
    assert "العقار 1: شقة راقية بسعر 2 مليون" in user_p
    assert "تعليمات الاستشارة الإلزامية" in user_p


def test_dynamic_metadata_injection():
    from real_estate.services.metadata_service import get_metadata_service
    from real_estate.schemas.intent import get_intent_function_calling_tool

    meta_service = get_metadata_service()
    mock_meta = {
        "locations": ["Alexandria, Smouha", "Cairo, New Cairo", "Giza, Sheikh Zayed"],
        "districts": ["سموحة", "التجمع الخامس", "الشيخ زايد"],
        "property_types": ["Apartment", "Villa", "Chalet"],
        "listing_types": ["Sale", "Rent"],
        "min_price": 500000.0,
        "max_price": 25000000.0
    }

    # 1. Test Prompt Injection
    base_prompt = "أنت محرك استخراج نية البحث العقاري."
    dynamic_prompt = meta_service.inject_metadata_into_prompt(base_prompt, mock_meta)
    assert "بيانات السوق والمخزون الحي" in dynamic_prompt
    assert "Apartment, Villa, Chalet" in dynamic_prompt
    assert "Sale, Rent" in dynamic_prompt
    assert "25,000,000" in dynamic_prompt

    # 2. Test Tool Schema Injection
    base_tool = get_intent_function_calling_tool()
    dynamic_tool = meta_service.inject_metadata_into_tool_schema(base_tool, mock_meta)
    props = dynamic_tool["function"]["parameters"]["properties"]
    assert props["property_type"]["enum"] == ["Apartment", "Villa", "Chalet"]
    assert props["listing_type"]["enum"] == ["Sale", "Rent"]
    assert "سموحة" in props["district"]["description"]

    # 3. Test Location Resolution
    city, district = meta_service.resolve_location_sync("شقة في سموحة للبيع")
    assert city == "alexandria"
    assert district == "سموحة"

    city2, district2 = meta_service.resolve_location_sync("فيلا في الشيخ زايد")
    assert city2 == "giza"
    assert district2 == "الشيخ زايد"


@pytest.mark.asyncio
async def test_validate_and_normalize_filters():
    from real_estate.services.metadata_service import get_metadata_service

    meta_service = get_metadata_service()
    raw = {
        "city": "alexandria",
        "district": "سموحة",
        "listing_type": "للبيع",
        "property_type": "شقة",
        "min_price": -50.0,
        "max_price": 3000000.0,
        "min_area_sqm": 3.0,  # Below 15 sqm threshold -> should be sanitized
        "bedrooms": 3
    }
    validated = await meta_service.validate_and_normalize_filters(raw)
    assert validated["location"] == "alexandria"
    assert validated["district"] == "سموحة"
    assert validated["listing_type"] == "Sale"
    assert validated["property_type"] == "Apartment"
    assert validated["max_price"] == 3000000.0
    assert "min_price" not in validated  # Negative discarded
    assert "min_area_sqm" not in validated  # Unrealistically small discarded
    assert validated["bedrooms"] == 3

