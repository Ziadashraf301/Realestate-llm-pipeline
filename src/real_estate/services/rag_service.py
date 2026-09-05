"""
Unified Enterprise RAG Service.
Encapsulates Intent Parsing, Milvus 2.5 Native Hybrid Search (Dense HNSW + BM25 Sparse + RRFRanker),
Cross-Encoder Re-ranking, LLM Generation (local llama.cpp primary, Gemini fallback),
and Two-Tier Caching (6-Hour TTL) into a single, cohesive service.

GENERATION PRIORITY ORDER:
  1. Native llama.cpp server (port 8080, Qwen 2.5 7B GGUF) — local, zero-cost, privacy-first
  2. Google Gemini 2.0 Flash (cloud fallback, requires GOOGLE_API_KEY)
  3. Deterministic safe HTML fallback (always available)

HYBRID SEARCH PATH (Milvus 2.5 native):
  Dense  : HNSW COSINE on 384-dim ONNX embedding
  Sparse : BM25 SPARSE_INVERTED_INDEX on 'text' field (no in-memory index, no rank_bm25)
  Fusion : RRFRanker(k=60) — native Milvus engine, zero Python loop
"""

import asyncio
import time
from typing import Any
import httpx
from markdown import markdown

from real_estate.core.settings import settings
from real_estate.core.logger import logger
from real_estate.core.tracing import MLflowTracer
from real_estate.core.prompt_registry import MLflowPromptRegistry
from real_estate.repositories.base import BaseVectorRepository
from real_estate.services.cache_service import TwoTierCacheService
from real_estate.services.intent_service import IntentService
from real_estate.services.metadata_service import get_metadata_service
from real_estate.retrieval.onnx_embedder import OnnxEmbeddingService
from real_estate.retrieval.cross_encoder import OnnxCrossEncoderService
from real_estate.schemas.property import PropertyRead
from real_estate.schemas.rag import RAGQueryRequest, RAGResponse


class RAGService:
    """
    Unified RAG Service.
    Integrates Milvus 2.5 Native Hybrid Search (Dense HNSW + BM25 Sparse + RRFRanker),
    Cross-Encoder Re-ranking, Conversational Generation (local llama.cpp primary),
    and Two-Tier Caching.
    """

    def __init__(
        self,
        cache_service: TwoTierCacheService,
        vector_repo: BaseVectorRepository,
        intent_service: IntentService,
        embedder: OnnxEmbeddingService | None = None,
        reranker: OnnxCrossEncoderService | None = None,
    ):
        self.cache_service = cache_service
        self.vector_repo = vector_repo
        self.intent_service = intent_service
        self.embedder = embedder or OnnxEmbeddingService()
        self.reranker = reranker or OnnxCrossEncoderService()
        # NOTE: ArabicBM25Retriever removed — replaced by Milvus 2.5 native BM25 sparse search.

        # Optional Gemini client (cloud fallback only, lazy-initialized)
        self._gemini_client = None
        self._gemini_model = None
        self._gemini_sdk = None
        if settings.GOOGLE_API_KEY:
            try:
                from google import genai
                self._gemini_client = genai.Client(api_key=settings.GOOGLE_API_KEY)
                self._gemini_sdk = "genai"
                logger.info("gemini_cloud_fallback_initialized", model=settings.GEMINI_MODEL, sdk="google.genai")
            except ImportError:
                try:
                    import warnings
                    with warnings.catch_warnings():
                        warnings.filterwarnings("ignore", category=FutureWarning)
                        import google.generativeai as legacy_genai
                        legacy_genai.configure(api_key=settings.GOOGLE_API_KEY)
                        self._gemini_model = legacy_genai.GenerativeModel(settings.GEMINI_MODEL)
                        self._gemini_sdk = "legacy"
                        logger.info("gemini_cloud_fallback_initialized", model=settings.GEMINI_MODEL, sdk="google.generativeai")
                except Exception as e:
                    logger.warning("gemini_cloud_fallback_init_failed", error=str(e))

    # -------------------------------------------------------------------------
    # Internal Helpers
    # -------------------------------------------------------------------------
    async def _extract_filter_dict(self, intent: Any | None) -> dict[str, Any]:
        """Helper to cleanly build and validate normalized filters against live ClickHouse inventory."""
        raw_filters: dict[str, Any] = {}
        numeric_fields = {"min_price", "max_price", "min_area_sqm", "max_area_sqm", "min_bedrooms", "min_bathrooms", "bedrooms", "bathrooms"}

        if intent:
            intent_filters = intent.to_filter_dict()
            for k, v in intent_filters.items():
                target_key = "location" if k == "city" else k
                if target_key not in raw_filters and v is not None:
                    if target_key in numeric_fields:
                        try:
                            if float(v) <= 0:
                                continue
                        except (ValueError, TypeError):
                            continue
                    raw_filters[target_key] = v

        # Validate and cross-check against ClickHouse live vocabulary
        validated_filters = await get_metadata_service().validate_and_normalize_filters(raw_filters)

        # Fix inverted area range if present
        if "min_area_sqm" in validated_filters and "max_area_sqm" in validated_filters:
            if float(validated_filters["min_area_sqm"]) > float(validated_filters["max_area_sqm"]):
                logger.warning("sanitizing_conflicting_area_filters", min_area=validated_filters["min_area_sqm"], max_area=validated_filters["max_area_sqm"])
                validated_filters.pop("max_area_sqm")

        return validated_filters

    async def _hybrid_search(self, query_text: str, filters: dict[str, Any], top_n: int) -> list[PropertyRead]:
        """
        Core Retrieval Pipeline:
          1. ONNX dense embedding of query text (384-dim, multilingual-e5-small)
          2. Milvus 2.5 native hybrid_search:
               - Dense  : HNSW COSINE ANN on 'vector' field
               - Sparse : BM25 ANN on 'sparse_vector' field (auto-generated from 'text')
               - Fusion : RRFRanker(k=60) — applied inside Milvus engine
               - Filter : single expr applied uniformly to both paths
          3. Cross-Encoder deep re-ranking (top_n final results)
        """
        with MLflowTracer.span(
            "hybrid_retrieval_and_rerank",
            span_type="RETRIEVER",
            inputs={"query": query_text, "filters": filters, "top_n": top_n}
        ) as retr_span:
            # 1. Dense Vector Embeddings (ONNX)
            with MLflowTracer.span("query_vector_embedding", span_type="EMBEDDING", inputs={"query_text": query_text}) as emb_span:
                t0_emb = time.perf_counter()
                query_vector_arr = await asyncio.to_thread(self.embedder.encode, query_text, is_query=True)
                query_vector = query_vector_arr.tolist()
                emb_latency = (time.perf_counter() - t0_emb) * 1000
                emb_span.set_outputs({"vector_dim": len(query_vector), "model": "multilingual-e5-small-int8", "latency_ms": round(emb_latency, 2)})
                logger.info(
                    "onnx_dense_embedding_complete",
                    query_preview=query_text[:60],
                    vector_dim=len(query_vector),
                    model="multilingual-e5-small-int8",
                    latency_ms=round(emb_latency, 2)
                )

            # 2. Milvus 2.5 Native Hybrid Search (Dense + BM25 Sparse + RRFRanker)
            t0_retrieval = time.perf_counter()
            with MLflowTracer.span(
                "milvus_hybrid_search",
                span_type="RETRIEVER",
                inputs={"query_text": query_text, "filters": filters, "top_k": 30}
            ) as milvus_span:
                hybrid_hits = await self.vector_repo.hybrid_search(
                    query_vector=query_vector,
                    query_text=query_text,
                    top_k=30,
                    filters=filters,
                )

                # Progressive Relaxation Fallback: if 0 hits match strict multi-scalar filters,
                # relax secondary filters (price/rooms/area) while STRICTLY PRESERVING location/city constraint.
                if not hybrid_hits and filters:
                    logger.info("milvus_zero_hits_retrying_with_relaxed_filters", original_filters=filters)
                    # City/Location is the #1 Hard Priority Constraint - never drop location if specified!
                    relaxed_filters = {k: v for k, v in filters.items() if k in ("location", "city", "listing_type", "property_type")}
                    if relaxed_filters != filters:
                        hybrid_hits = await self.vector_repo.hybrid_search(
                            query_vector=query_vector,
                            query_text=query_text,
                            top_k=30,
                            filters=relaxed_filters,
                        )
                    # If still 0 hits, relax property_type/listing_type but STRICTLY STAY within the target city
                    if not hybrid_hits and ("location" in filters or "city" in filters):
                        city_only_filters = {k: v for k, v in filters.items() if k in ("location", "city")}
                        if city_only_filters != relaxed_filters:
                            logger.info("milvus_zero_hits_retrying_within_same_city_only", city_filter=city_only_filters)
                            hybrid_hits = await self.vector_repo.hybrid_search(
                                query_vector=query_vector,
                                query_text=query_text,
                                top_k=30,
                                filters=city_only_filters,
                            )
                    # Only if NO location was specified by the user at all, fall back to unrestricted search
                    elif not hybrid_hits and not ("location" in filters or "city" in filters):
                        logger.info("milvus_zero_hits_retrying_unrestricted_search")
                        hybrid_hits = await self.vector_repo.hybrid_search(
                            query_vector=query_vector,
                            query_text=query_text,
                            top_k=30,
                            filters=None,
                        )

                retrieval_latency = (time.perf_counter() - t0_retrieval) * 1000
                milvus_span.set_outputs({
                    "candidates_retrieved": len(hybrid_hits),
                    "latency_ms": round(retrieval_latency, 2),
                    "fusion_ranker": "RRFRanker(k=60)",
                    "top_scores": [round(float(h.get("similarity", 0.0)), 4) for h in hybrid_hits[:5]],
                    "candidate_ids": [h.get("id") for h in hybrid_hits[:5]]
                })
            logger.info(
                "milvus_hybrid_retrieval_finished",
                query_preview=query_text[:60],
                filters_applied=filters,
                candidates_retrieved=len(hybrid_hits),
                latency_ms=round(retrieval_latency, 2)
            )

            # 3. Cross-Encoder Deep Re-ranking (bge-reranker-base INT8)
            t0_rerank = time.perf_counter()
            reranked_hits = await asyncio.to_thread(
                self.reranker.rerank,
                query=query_text,
                candidates=hybrid_hits,
                top_n=top_n,
            )
            rerank_latency = (time.perf_counter() - t0_rerank) * 1000
            logger.info(
                "cross_encoder_rerank_finished",
                candidates_in=len(hybrid_hits),
                top_n_out=len(reranked_hits),
                latency_ms=round(rerank_latency, 2),
                top_scores=[round(float(h.get("rerank_score", 0.0)), 3) for h in reranked_hits[:3]]
            )

            results = [
                PropertyRead(
                    id=h.get("id", f"prop_{i}"),
                    title=h.get("title", "عقار بدون عنوان"),
                    location=h.get("location", "alexandria"),
                    listing_type=h.get("listing_type", "تمليك"),
                    property_type=h.get("property_type", "شقة"),
                    price_egp=float(h.get("price_egp", 0.0)),
                    bedrooms=h.get("bedrooms"),
                    bathrooms=h.get("bathrooms"),
                    area_sqm=h.get("area_sqm"),
                    description=h.get("text") or h.get("description"),
                    url=h.get("url"),
                    similarity=h.get("rerank_score") or h.get("similarity"),
                )
                for i, h in enumerate(reranked_hits)
            ]

            retr_span.set_outputs({
                "candidates_retrieved": len(hybrid_hits),
                "top_reranked_count": len(results),
                "top_score": results[0].similarity if results else 0.0
            })
            return results


    # -------------------------------------------------------------------------
    # 2. LLM Generation — Local llama.cpp PRIMARY, Gemini Cloud FALLBACK
    # -------------------------------------------------------------------------
    async def _generate_via_llama_cpp(self, prompt: str) -> str | None:
        """
        Calls native local llama.cpp C++ server (port 8080, Qwen 2.5 7B GGUF).
        Primary LLM: zero-cost, fully local, privacy-preserving.
        """
        t0 = time.perf_counter()
        with MLflowTracer.span(
            "generation_llama_cpp",
            span_type="CHAT_MODEL",
            inputs={"prompt_preview": prompt[:300], "engine": "llama.cpp"}
        ) as llm_span:
            try:
                async with httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=0.5)) as client:
                    resp = await client.post(
                        f"{settings.LLAMA_CPP_ENDPOINT}/v1/chat/completions",
                        json={
                            "model": "qwen2.5-7b-instruct",
                            "messages": [
                                {"role": "system", "content": MLflowPromptRegistry.get_advisor_system_instruction()},
                                {"role": "user", "content": prompt}
                            ],
                            "temperature": 0.3,
                            "max_tokens": 700,
                            "stream": False
                        }
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        content = data["choices"][0]["message"]["content"]
                        usage = data.get("usage", {})
                        latency_ms = (time.perf_counter() - t0) * 1000
                        MLflowTracer.log_llm_generation(
                            engine="llama.cpp",
                            prompt=prompt,
                            completion=content,
                            latency_ms=latency_ms,
                            prompt_tokens=usage.get("prompt_tokens"),
                            completion_tokens=usage.get("completion_tokens")
                        )
                        llm_span.set_outputs({"status": "success", "completion_preview": content[:200]})
                        return markdown(content)
                    else:
                        logger.warning("llama_cpp_non_200_response", status=resp.status_code)
                        llm_span.set_attribute("http_status", resp.status_code)
                        return None
            except Exception as e:
                logger.warning("llama_cpp_server_unavailable", error=str(e))
                llm_span.set_attribute("error", str(e))
                return None

    async def _generate_via_gemini(self, prompt: str) -> str | None:
        """
        Calls Google Gemini 2.0 Flash (cloud fallback).
        Only used when llama.cpp is unavailable or returns an error.
        Requires GOOGLE_API_KEY to be set.
        """
        if not self._gemini_client and not self._gemini_model:
            return None
        t0 = time.perf_counter()
        with MLflowTracer.span(
            "generation_gemini_fallback",
            span_type="CHAT_MODEL",
            inputs={"prompt_preview": prompt[:300], "engine": settings.GEMINI_MODEL}
        ) as llm_span:
            try:
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
                            temperature=0.3,
                            max_output_tokens=800
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
                                temperature=0.3,
                                max_output_tokens=800
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
                    prompt=prompt,
                    completion=content or "",
                    latency_ms=latency_ms,
                    prompt_tokens=p_tokens,
                    completion_tokens=c_tokens
                )
                llm_span.set_outputs({"status": "success", "completion_preview": (content or "")[:200]})
                return markdown(content or "")
            except Exception as e:
                logger.warning("gemini_cloud_generation_failed", error=str(e))
                llm_span.set_attribute("error", str(e))
                return None

    async def generate_recommendation(
        self,
        query: str,
        properties: list[dict[str, Any]],
    ) -> str:
        """
        Synthesizes structured context and calls LLM engine.
        Follows strict fallback order: local llama.cpp -> Gemini -> safe HTML template.
        """
        with MLflowTracer.span(
            "grounded_advisor_generation",
            span_type="CHAT_MODEL",
            inputs={"query": query, "candidate_properties": len(properties)}
        ) as gen_span:
            if not properties:
                out = "<p>لم يتم العثور على عقارات مطابقة لبحثك في قاعدة البيانات حالياً. يرجى تجربة معايير بحث أخرى.</p>"
                gen_span.set_outputs({"recommendation_html": out})
                return out

            context_chunks = []
            for idx, p in enumerate(properties[:5], 1):
                chunk = (
                    f"عقار #{idx}: {p.get('title', 'غير محدد')}\n"
                    f"• الموقع: {p.get('location', 'غير محدد')}\n"
                    f"• السعر: {p.get('price_egp', 0):,} جنيه\n"
                    f"• الغرف: {p.get('bedrooms', 'غير محدد')} | الحمامات: {p.get('bathrooms', 'غير محدد')} | المساحة: {p.get('area_sqm', 'غير محدد')} م²\n"
                    f"• النوع: {p.get('listing_type', '')} - {p.get('property_type', '')}\n"
                    f"• الوصف: {(p.get('description') or p.get('text') or '')[:300]}..."
                )
                context_chunks.append(chunk)

            context_text = "\n\n".join(context_chunks)
            prompt = MLflowPromptRegistry.build_advisor_user_prompt(query, context_text)

            # 1. PRIMARY: Local llama.cpp (Qwen 2.5 7B GGUF, port 8080) — zero cost
            result = await self._generate_via_llama_cpp(prompt)
            if result:
                gen_span.set_outputs({"engine": "llama.cpp", "recommendation_html": result[:200]})
                return result

            # 2. FALLBACK: Cloud Gemini 2.0 Flash (only if llama.cpp is down)
            result = await self._generate_via_gemini(prompt)
            if result:
                gen_span.set_outputs({"engine": "gemini-2.0-flash", "recommendation_html": result[:200]})
                return result

            # 3. Safe deterministic HTML fallback (always available)
            fallback = f"<p>تم العثور على {len(properties)} عقار مطابق لطلبك. يمكنك مراجعة بطاقات العقارات أدناه للحصول على التفاصيل المباشرة.</p>"
            gen_span.set_outputs({"engine": "deterministic_fallback", "recommendation_html": fallback})
            return fallback

    # -------------------------------------------------------------------------
    # 3. End-to-End RAG Pipeline (Retrieval + Generation + Caching)
    # -------------------------------------------------------------------------
    async def execute_rag(self, req: RAGQueryRequest) -> RAGResponse:
        """Executes full RAG workflow with Two-Tier Caching and rich observability."""
        with MLflowTracer.span(
            "real_estate_rag_pipeline",
            span_type="CHAIN",
            inputs={"query": req.query, "n_results": req.n_results, "bypass_cache": req.bypass_cache}
        ) as root_trace:
            start_time = time.perf_counter()
            raw_query = req.query.strip()
            cache_key = f"rag:{raw_query}"

            logger.info("rag_pipeline_execution_started", query=raw_query, n_results=req.n_results, bypass_cache=req.bypass_cache)

            # 1. Tier-1 Fast Exact Cache Check (<2ms, skips LLM intent extraction completely)
            if not req.bypass_cache and hasattr(self.cache_service, "get_exact"):
                exact_hit = await self.cache_service.get_exact(cache_key)
                if exact_hit and exact_hit.get("data"):
                    data = exact_hit["data"]
                    props_raw = data.get("properties", [])
                    if props_raw:  # Only serve non-empty cached responses
                        latency = (time.perf_counter() - start_time) * 1000
                        logger.info("rag_pipeline_tier1_instant_hit", query=raw_query, latency_ms=round(latency, 2), results_count=len(props_raw))
                        response = RAGResponse(
                            success=True,
                            query=raw_query,
                            intent=data.get("intent"),
                            recommendation=data.get("recommendation", ""),
                            properties=[PropertyRead(**p) for p in props_raw],
                            cached=True,
                            cache_tier="exact_cache",
                            similarity_score=1.0,
                            latency_ms=round(latency, 2),
                        )
                        root_trace.set_outputs({
                            "cached": True,
                            "cache_tier": "exact_cache",
                            "results_count": len(response.properties),
                            "latency_ms": round(latency, 2)
                        })
                        return response

            # 2. Intent extraction & filter normalization
            intent = await self.intent_service.extract_intent(raw_query)
            filters = await self._extract_filter_dict(intent)
            logger.info("rag_pipeline_intent_ready", query=raw_query, filters=filters)

            # 3. Check Two-Tier Cache (Tier 1 with filters + Tier 2 Semantic Milvus Cache)
            cached_result = await self.cache_service.get(cache_key, filters=filters, bypass=req.bypass_cache)
            if cached_result:
                data = cached_result["data"]
                props_raw = data.get("properties", []) if isinstance(data, dict) else []
                if props_raw:
                    latency = (time.perf_counter() - start_time) * 1000
                    logger.info("rag_pipeline_cache_hit", query=raw_query, source=cached_result["source"], latency_ms=round(latency, 2))
                    response = RAGResponse(
                        success=True,
                        query=raw_query,
                        intent=data.get("intent") if isinstance(data, dict) else intent,
                        recommendation=data.get("recommendation", "") if isinstance(data, dict) else "",
                        properties=[PropertyRead(**p) for p in props_raw],
                        cached=True,
                        cache_tier=cached_result["source"],
                        similarity_score=cached_result.get("similarity"),
                        latency_ms=round(latency, 2),
                    )
                    root_trace.set_outputs({
                        "cached": True,
                        "cache_tier": cached_result["source"],
                        "results_count": len(response.properties),
                        "latency_ms": round(latency, 2)
                    })
                    return response

            # 4. Core Hybrid Retrieval (Dense Milvus + Sparse BM25 + RRF + Cross-Encoder)
            query_text = intent.cleaned_semantic_query if intent else raw_query
            properties = await self._hybrid_search(query_text, filters, top_n=req.n_results)
            properties_dict = [p.model_dump() for p in properties]
            logger.info("rag_pipeline_retrieval_complete", retrieved_count=len(properties))

            # 5. Generate grounded Arabic recommendation (llama.cpp → Gemini → safe fallback)
            recommendation_html = await self.generate_recommendation(
                query=raw_query,
                properties=properties_dict,
            )

            # 6. Store non-empty RAG payload in Two-Tier Cache with filter isolation
            if properties_dict:
                cache_payload = {
                    "intent": intent.model_dump() if intent else None,
                    "recommendation": recommendation_html,
                    "properties": properties_dict,
                }
                await self.cache_service.set(cache_key, cache_payload, filters=filters)

            latency = (time.perf_counter() - start_time) * 1000
            response = RAGResponse(
                success=True,
                query=raw_query,
                intent=intent,
                recommendation=recommendation_html,
                properties=properties,
                cached=False,
                cache_tier=None,
                similarity_score=None,
                latency_ms=round(latency, 2),
            )
            root_trace.set_outputs({
                "cached": False,
                "results_count": len(properties),
                "latency_ms": round(latency, 2),
                "recommendation_preview": recommendation_html[:150],
            })
            logger.info("rag_pipeline_execution_finished", latency_ms=round(latency, 2), results_count=len(properties))
            return response
