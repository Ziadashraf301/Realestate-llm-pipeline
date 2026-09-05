"""
Query Intent Extraction Schemas (Pydantic v2 & Milvus Metadata Alignment).
Defines the exact structured representation of user constraints aligned with the Milvus vector schema.
Provides JSON schema and Function Calling tool definitions for LLM routing.
"""

from typing import Literal, Optional, Dict, Any
from pydantic import BaseModel, Field

# Validated Enums aligned with Milvus Collection Schema
CityType = Literal["alexandria", "cairo", "giza"]
ListingType = Literal["Sale", "Rent"]
PropertyType = Literal[
    "Apartment",
    "Villa",
    "Duplex",
    "Penthouse",
    "Chalet",
    "Townhouse",
    "Studio",
    "Twin House",
    "Building",
    "Commercial",
    "Land"
]


class ExtractedQueryIntent(BaseModel):
    """Structured extraction of user search intent matching the Milvus vector collection fields."""

    city: Optional[CityType] = Field(
        None,
        description="المحافظة أو المدينة الرئيسية: إما 'alexandria' أو 'cairo' أو 'giza'."
    )
    district: Optional[str] = Field(
        None,
        description="الحي أو المنطقة داخل المدينة (مثل: سموحة، ستانلي، لوران، ميامي، التجمع الخامس، المعادي، الشيخ زايد، 6 أكتوبر)."
    )
    listing_type: Optional[ListingType] = Field(
        None,
        description="طبيعة المعاملة: 'Sale' للبيع/تمليك، أو 'Rent' للإيجار."
    )
    property_type: Optional[PropertyType] = Field(
        None,
        description="نوع العقار: 'Apartment' (شقة), 'Villa' (فيلا), 'Duplex' (دوبلكس), 'Chalet' (شاليه), إلخ."
    )
    min_price: Optional[float] = Field(
        None,
        description="الحد الأدنى للسعر بالجنيه المصري (EGP)."
    )
    max_price: Optional[float] = Field(
        None,
        description="الحد الأقصى للميزانية أو السعر بالجنيه المصري (EGP)."
    )
    min_bedrooms: Optional[int] = Field(
        None,
        description="الحد الأدنى لعدد غرف النوم المطلوبة."
    )
    min_bathrooms: Optional[int] = Field(
        None,
        description="الحد الأدنى لعدد الحمامات المطلوبة."
    )
    min_area_sqm: Optional[float] = Field(
        None,
        description="الحد الأدنى للمساحة بالمتر المربع."
    )
    max_area_sqm: Optional[float] = Field(
        None,
        description="الحد الأقصى للمساحة بالمتر المربع."
    )
    cleaned_semantic_query: str = Field(
        ...,
        description="النص الدلالي الصافي بعد عزل الفلاتر الرقمية، لاستخدامه في البحث المتجهي الكثيف (Dense Embedding)."
    )

    @property
    def bedrooms(self) -> Optional[int]:
        return self.min_bedrooms

    @property
    def bathrooms(self) -> Optional[int]:
        return self.min_bathrooms

    def to_filter_dict(self) -> Dict[str, Any]:
        """Converts intent to non-null filter dictionary for Milvus and BM25 search."""
        data = self.model_dump(exclude_none=True)
        data.pop("cleaned_semantic_query", None)
        return data


def get_intent_function_calling_tool() -> Dict[str, Any]:
    """Generates standard OpenAI-compatible tool definition for function calling."""
    return {
        "type": "function",
        "function": {
            "name": "extract_real_estate_intent",
            "description": "استخراج محددات وفلاتر البحث العقاري المصري بدقة وفق مخطط الحقول في قاعدة بيانات Milvus.",
            "parameters": ExtractedQueryIntent.model_json_schema()
        }
    }
