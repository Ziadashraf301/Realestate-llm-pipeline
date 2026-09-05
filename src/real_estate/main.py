"""
FastAPI Enterprise Application Master Entrypoint.
Responsible strictly for: Lifespan orchestration, Middleware registration, and Router mounting.
Strictly adheres to Single Responsibility Principle (SRP).
"""

from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from prometheus_client import make_asgi_app

from real_estate.core.settings import settings
from real_estate.core.logger import logger
from real_estate.api.middleware import TimingAndCorrelationMiddleware
from real_estate.api.v1.api import api_v1_router
from real_estate.api.v1.endpoints.health import router as health_router
from real_estate.api.web_router import web_router
import httpx
from real_estate.core.redis import close_redis_pool
from real_estate.services.user_seed_service import seed_initial_users
from real_estate.api.deps import (
    get_cache_repository,
    get_vector_repository,
    get_intent_service,
    get_user_repository,
)
from real_estate.retrieval.onnx_embedder import OnnxEmbeddingService
from real_estate.retrieval.cross_encoder import OnnxCrossEncoderService


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initializes connection pools, singletons, and AI models on startup."""
    logger.info(
        "starting_real_estate_fastapi_server",
        environment=settings.ENVIRONMENT,
        milvus_host=settings.MILVUS_HOST,
        redis_host=settings.REDIS_HOST,
        caching_ttl=f"{settings.CACHE_TTL_SECONDS}s (6h)",
        docs_url="/docs"
    )
    # 1. Warm up repository singletons, initialize Milvus collection schema & seed RBAC roles
    get_cache_repository()
    vector_repo = get_vector_repository()
    user_repo = get_user_repository()
    await seed_initial_users(user_repo)
    logger.info("rbac_seed_users_verified")

    # 1b. Ensure Milvus collection has hybrid schema (sparse_vector + BM25 Function)
    #     Safe no-op if collection already exists with correct schema.
    if hasattr(vector_repo, "initialize_collection"):
        vector_repo.initialize_collection()
        logger.info("milvus_hybrid_collection_schema_verified")

    # 2. Warm up AI / ML inference models (ONNX Runtime INT8)
    logger.info("warming_up_ai_models")
    try:
        # a. ONNX Dense Embedder (multilingual-e5-small INT8)
        embedder = OnnxEmbeddingService()
        _ = embedder.encode("warmup", is_query=True)
        logger.info("onnx_embedder_warmed_up")

        # b. ONNX Cross-Encoder Re-ranker (bge-reranker-base INT8)
        reranker = OnnxCrossEncoderService()
        _ = reranker.rerank("warmup", [{"text": "sample property"}], top_n=1)
        logger.info("onnx_cross_encoder_reranker_warmed_up")

        # c. Intent Extraction Service
        get_intent_service()
        logger.info("ai_models_warmed_up_successfully")
    except Exception as e:
        logger.warning("ai_model_warmup_partial_failure", error=str(e))

    # 3. Probe LLM Generation Engine (Local llama.cpp vs Cloud Gemini fallback)
    llm_status = "Deterministic Template Mode"
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            resp = await client.get(f"{settings.LLAMA_CPP_ENDPOINT}/health")
            if resp.status_code == 200:
                llm_status = "Native llama.cpp (Local CPU/GPU port 8080) [Primary Active]"
                logger.info(
                    "llm_generation_probe_success",
                    engine="llama.cpp",
                    endpoint=settings.LLAMA_CPP_ENDPOINT,
                    mode="Primary Local",
                )
            else:
                raise RuntimeError(f"llama.cpp health returned HTTP {resp.status_code}")
    except Exception as e:
        logger.info("llama_cpp_probe_offline_testing_fallback", error=str(e))
        if settings.GOOGLE_API_KEY:
            llm_status = "Google Gemini 2.0 Flash [Cloud Fallback Active]"
            logger.info(
                "llm_generation_probe_fallback",
                engine="Google Gemini 2.0 Flash",
                mode="Cloud Fallback",
            )
        else:
            llm_status = "Deterministic Template Mode (No LLM Backend Available)"
            logger.warning("llm_generation_no_backend_available")

    app.state.llm_engine_status = llm_status

    yield
    logger.info("shutting_down_real_estate_fastapi_server")
    await close_redis_pool()


def create_application() -> FastAPI:
    """Application factory assembling routers, middlewares, and static mounts."""
    app = FastAPI(
        title="Real Estate Intelligence System",
        description="Enterprise Real Estate Discovery, Agentic RAG, and Two-Tier Caching Platform.",
        version="2.0.0",
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc"
    )

    # 1. Middlewares
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["X-Cache", "X-Process-Time-ms", "X-Request-ID"]
    )
    app.add_middleware(TimingAndCorrelationMiddleware)

    # 2. Static Assets Mount
    static_dir = Path(__file__).resolve().parent / "web" / "static"
    if static_dir.exists():
        app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    # 3. Router & Metrics Mounting (Separation of Concerns: zero route logic in main.py)
    app.include_router(health_router)  # Root /health & /stats for Docker healthcheck and monitoring
    app.include_router(web_router)
    app.include_router(api_v1_router)

    # 4. Prometheus Metrics Exporter
    app.mount("/metrics", make_asgi_app())

    return app


# Master ASGI Application Instance
app = create_application()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "real_estate.main:app",
        host=settings.APP_HOST,
        port=settings.APP_PORT,
        reload=settings.DEBUG
    )
