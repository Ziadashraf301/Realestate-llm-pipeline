"""
Query Intent Extraction & Semantic Routing Service (Open Source LLM Primary + Cloud Fallback).
Extracts structured search constraints matching the Milvus 12-field vector metadata schema.

Routing Hierarchy:
1. PRIMARY: Local small open-source Arabic model via native llama.cpp (port 8080) with JSON Schema output.
2. FALLBACK: Google Gemini 2.0 / 3.x Flash via cloud API.
3. SAFETY NET: Deterministic schema-aligned parser for Egyptian colloquial real estate queries.
"""

import json
import re
import time
import asyncio
from typing import Optional, Dict, Any, Tuple, cast
from pydantic import ValidationError
import httpx

from real_estate.core.settings import settings
from real_estate.core.logger import logger
from real_estate.core.tracing import MLflowTracer
from real_estate.core.prompt_registry import MLflowPromptRegistry
from real_estate.schemas.intent import ExtractedQueryIntent, CityType
from real_estate.services.metadata_service import get_metadata_service

# Pre-compiled Regex Patterns for High-Throughput Parsing
RE_PRICE_MILLION = re.compile(r"(\d+(?:\.\d+)?)\s*مليون")
RE_PRICE_THOUSAND = re.compile(r"(\d+(?:\.\d+)?)\s*ألف")
RE_BEDROOMS = re.compile(r"(\d+)\s*(?:غرف|أوض|أوضة|نوم)")
RE_AREA = re.compile(r"(\d+(?:\.\d+)?)\s*(?:متر(?:\s*مربع)?|م²|م2)(?![\wء-ي]*ليون)")


class IntentService:
    """Enterprise Query Routing and Intent Extraction Service."""

    def __init__(self):
        self._gemini_client = None
        self._gemini_model = None
        self._gemini_sdk = None
        if settings.GOOGLE_API_KEY:
            # 1. Try modern google.genai SDK
            try:
                from google import genai
                self._gemini_client = genai.Client(api_key=settings.GOOGLE_API_KEY)
                self._gemini_sdk = "genai"
                logger.info("intent_service_google_genai_fallback_ready", model=settings.GEMINI_MODEL, sdk="google.genai")
            except ImportError:
                # 2. Fall back to legacy google.generativeai with FutureWarning suppressed
                try:
                    import warnings
                    with warnings.catch_warnings():
                        warnings.filterwarnings("ignore", category=FutureWarning)
                        import google.generativeai as legacy_genai
                        legacy_genai.configure(api_key=settings.GOOGLE_API_KEY)
                        self._gemini_model = legacy_genai.GenerativeModel(settings.GEMINI_MODEL)
                        self._gemini_sdk = "legacy"
                        logger.info("intent_service_gemini_fallback_ready", model=settings.GEMINI_MODEL, sdk="google.generativeai")
                except Exception as e:
                    logger.warning("gemini_fallback_init_failed", error=str(e))

    async def _extract_via_llama_cpp(self, query: str) -> Optional[ExtractedQueryIntent]:
        """
        Calls primary local llama.cpp C++ server (port 8080) with JSON schema constraint
        and live ClickHouse database inventory context.
        """
        meta_service = get_metadata_service()
        metadata = await meta_service.get_live_metadata()
        base_prompt = MLflowPromptRegistry.get_intent_prompt()
        system_prompt = meta_service.inject_metadata_into_prompt(base_prompt, metadata)
        t0 = time.perf_counter()

        with MLflowTracer.span("intent_llama_cpp", span_type="CHAT_MODEL", inputs={"query": query}) as llm_span:
            try:
                async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=0.5)) as client:
                    resp = await client.post(
                        f"{settings.LLAMA_CPP_ENDPOINT}/v1/chat/completions",
                        json={
                            "model": "qwen2.5-7b-instruct",
                            "messages": [
                                {"role": "system", "content": system_prompt},
                                {"role": "user", "content": f"طلب العميل: \"{query}\""}
                            ],
                            "response_format": {"type": "json_object"},
                            "temperature": 0.1,
                            "max_tokens": 450,
                            "stream": False
                        }
                    )
                    latency_ms = (time.perf_counter() - t0) * 1000
                    if resp.status_code == 200:
                        data = resp.json()
                        raw_content = data["choices"][0]["message"]["content"]
                        usage = data.get("usage", {})
                        
                        clean_content = raw_content.strip()
                        if clean_content.startswith("```json"):
                            clean_content = clean_content[7:]
                        elif clean_content.startswith("```"):
                            clean_content = clean_content[3:]
                        if clean_content.endswith("```"):
                            clean_content = clean_content[:-3]
                        clean_content = clean_content.strip()

                        # Fallback to finding outermost JSON braces
                        if not (clean_content.startswith("{") and clean_content.endswith("}")):
                            start_idx = clean_content.find("{")
                            end_idx = clean_content.rfind("}")
                            if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
                                clean_content = clean_content[start_idx:end_idx + 1]

                        parsed = json.loads(clean_content)
                        intent = ExtractedQueryIntent(**parsed)
                        MLflowTracer.log_llm_generation(
                            engine="llama.cpp",
                            prompt=f"Intent extraction for: {query}",
                            completion=raw_content,
                            latency_ms=latency_ms,
                            prompt_tokens=usage.get("prompt_tokens"),
                            completion_tokens=usage.get("completion_tokens")
                        )
                        llm_span.set_outputs({"status": "success", "intent": intent.model_dump()})
                        logger.info(
                            "intent_extracted_via_llama_cpp",
                            city=intent.city,
                            district=intent.district,
                            listing_type=intent.listing_type,
                            property_type=intent.property_type,
                            max_price=intent.max_price,
                            latency_ms=round(latency_ms, 2)
                        )
                        return intent
                    else:
                        logger.warning(
                            "llama_cpp_intent_returned_non_200",
                            status=resp.status_code,
                            body=resp.text[:200],
                            latency_ms=round(latency_ms, 2)
                        )
                        llm_span.set_attribute("http_status", resp.status_code)
                        return None
            except Exception as e:
                latency_ms = (time.perf_counter() - t0) * 1000
                logger.info(
                    "llama_cpp_intent_unavailable_switching_to_gemini",
                    endpoint=settings.LLAMA_CPP_ENDPOINT,
                    error=str(e),
                    latency_ms=round(latency_ms, 2)
                )
                llm_span.set_attribute("fallback_triggered", str(e))
                return None

    async def _extract_via_gemini(self, query: str) -> Optional[ExtractedQueryIntent]:
        """Calls Google Gemini cloud API fallback with live ClickHouse inventory and JSON schema."""
        if not self._gemini_client and not self._gemini_model:
            return None

        meta_service = get_metadata_service()
        metadata = await meta_service.get_live_metadata()
        base_prompt = MLflowPromptRegistry.get_intent_prompt()
        system_prompt = meta_service.inject_metadata_into_prompt(base_prompt, metadata)
        t0 = time.perf_counter()

        with MLflowTracer.span("intent_gemini_fallback", span_type="CHAT_MODEL", inputs={"query": query}) as llm_span:
            try:
                prompt = f"{system_prompt}\n\nطلب العميل المطلوب تحليله: \"{query}\""
                content = None
                p_tokens = None
                c_tokens = None

                if self._gemini_sdk == "genai" and self._gemini_client is not None:
                    from google.genai import types
                    response = await asyncio.to_thread(
                        self._gemini_client.models.generate_content,
                        model=settings.GEMINI_MODEL,
                        contents=prompt,
                        config=types.GenerateContentConfig(
                            temperature=0.1,
                            max_output_tokens=450,
                            response_mime_type="application/json"
                        )
                    )
                    content = response.text
                    usage = getattr(response, "usage_metadata", None)
                    p_tokens = getattr(usage, "prompt_token_count", None) if usage else None
                    c_tokens = getattr(usage, "candidates_token_count", None) if usage else None
                elif self._gemini_model is not None:
                    import warnings
                    with warnings.catch_warnings():
                        warnings.filterwarnings("ignore", category=FutureWarning)
                        import google.generativeai as legacy_genai
                        response = await asyncio.to_thread(
                            self._gemini_model.generate_content,
                            prompt,
                            generation_config=legacy_genai.types.GenerationConfig(
                                temperature=0.1,
                                max_output_tokens=450,
                                response_mime_type="application/json"
                            )
                        )
                        content = response.text
                        usage = getattr(response, "usage_metadata", None)
                        p_tokens = getattr(usage, "prompt_token_count", None) if usage else None
                        c_tokens = getattr(usage, "candidates_token_count", None) if usage else None
                else:
                    return None

                latency_ms = (time.perf_counter() - t0) * 1000
                MLflowTracer.log_llm_generation(
                    engine=settings.GEMINI_MODEL,
                    prompt=prompt[:250],
                    completion=content,
                    latency_ms=latency_ms,
                    prompt_tokens=p_tokens,
                    completion_tokens=c_tokens
                )

                parsed = json.loads(content)
                intent = ExtractedQueryIntent(**parsed)
                llm_span.set_outputs({"status": "success", "intent": intent.model_dump()})
                logger.info(
                    "intent_extracted_via_gemini_fallback",
                    city=intent.city,
                    district=intent.district,
                    listing_type=intent.listing_type,
                    property_type=intent.property_type,
                    max_price=intent.max_price,
                    latency_ms=round(latency_ms, 2)
                )
                return intent
            except Exception as e:
                latency_ms = (time.perf_counter() - t0) * 1000
                logger.warning("gemini_intent_extraction_failed", error=str(e), latency_ms=round(latency_ms, 2))
                llm_span.set_attribute("error", str(e))
                return None

    @staticmethod
    def _deterministic_schema_parser(
        query: str,
        resolved_loc: Optional[Tuple[Optional[str], Optional[str]]] = None
    ) -> ExtractedQueryIntent:
        """
        Deterministic schema parser mapping Egyptian Arabic queries to Milvus schema.
        Dynamically uses resolved (city, district) from ClickHouse warehouse if available.
        Ensures 100% system availability if all LLMs are unreachable.
        """
        city = None
        district = None

        if resolved_loc and (resolved_loc[0] or resolved_loc[1]):
            city, district = resolved_loc
        else:
            city, district = get_metadata_service().resolve_location_sync(query)

        # Transaction type
        listing_type = "Rent" if any(w in query for w in ["إيجار", "ايجار", "للإيجار", "للايجار"]) else "Sale"

        # Property type
        property_type = "Apartment"
        if any(w in query for w in ["فيلا", "فيلات", "villa"]):
            property_type = "Villa"
        elif any(w in query for w in ["دوبلكس", "duplex"]):
            property_type = "Duplex"
        elif any(w in query for w in ["بنتهاوس", "penthouse"]):
            property_type = "Penthouse"
        elif any(w in query for w in ["شاليه", "chalet"]):
            property_type = "Chalet"
        elif any(w in query for w in ["تاون هاوس", "townhouse", "توين"]):
            property_type = "Townhouse"
        elif any(w in query for w in ["مكتب", "محل", "تجاري", "إداري"]):
            property_type = "Commercial"

        # Price ceiling
        max_price = None
        price_match = RE_PRICE_MILLION.search(query)
        if price_match:
            max_price = float(price_match.group(1)) * 1_000_000.0
        else:
            price_match2 = RE_PRICE_THOUSAND.search(query)
            if price_match2:
                max_price = float(price_match2.group(1)) * 1_000.0

        # Minimum bedrooms
        min_bedrooms = None
        rooms_match = RE_BEDROOMS.search(query)
        if rooms_match:
            min_bedrooms = int(rooms_match.group(1))

        # Minimum area
        min_area = None
        area_match = RE_AREA.search(query)
        if area_match:
            min_area = float(area_match.group(1))

        return ExtractedQueryIntent(
            city=cast(Optional[CityType], city),
            district=district,
            listing_type=listing_type,
            property_type=property_type,
            max_price=max_price,
            min_bedrooms=min_bedrooms,
            min_area_sqm=min_area,
            cleaned_semantic_query=query
        )

    async def extract_intent(self, query: str) -> ExtractedQueryIntent:
        """
        Executes intent extraction with hierarchical fallback:
        1. Local llama.cpp (Primary Open-Source Model)
        2. Google Gemini (Cloud Fallback)
        3. Deterministic Egyptian Dialect Parser (Safety Net)
        """
        with MLflowTracer.span("intent_routing_and_extraction", span_type="TOOL", inputs={"query": query}) as root_intent_span:
            # 1. Primary: Local llama.cpp
            intent = await self._extract_via_llama_cpp(query)
            if intent is not None:
                root_intent_span.set_outputs({"engine": "llama.cpp", "intent": intent.model_dump()})
                return intent

            # 2. Fallback: Google Gemini
            intent = await self._extract_via_gemini(query)
            if intent is not None:
                root_intent_span.set_outputs({"engine": "gemini", "intent": intent.model_dump()})
                return intent

            # 3. Deterministic Safety Net with ClickHouse metadata resolution
            logger.info("using_deterministic_intent_safety_net", query=query)
            resolved_loc = await get_metadata_service().resolve_location(query)
            intent = self._deterministic_schema_parser(query, resolved_loc=resolved_loc)
            root_intent_span.set_outputs({"engine": "deterministic_safety_net", "intent": intent.model_dump()})
            return intent

    def parse_intent(self, query: str) -> ExtractedQueryIntent:
        """Synchronous deterministic schema extraction (used by fast paths & offline tests)."""
        return self._deterministic_schema_parser(query)
