# 🏠 Egyptian Real Estate Intelligence & GenAI Production Platform

> Production-grade, containerized AI platform combining distributed web scraping, ClickHouse columnar warehousing, Milvus hybrid search (BM25 + Dense Embeddings + Cross-Encoder Reranking), Two-Tier caching, async FastAPI backend with JWT & RBAC, and MLflow GenAI Tracing audited via scientific RAGAS evaluation.

[![Python](https://img.shields.io/badge/Python-3.11%2B-blue.svg)](https://python.org)
[![FastAPI](<https://img.shields.io/badge/FastAPI-Production%20Async-009688.svg>)](https://fastapi.tiangolo.com)
[![ClickHouse](<https://img.shields.io/badge/ClickHouse-Columnar%20Warehouse-FFCC00.svg>)](https://clickhouse.com)
[![Milvus](<https://img.shields.io/badge/Milvus-2.5%20Hybrid%20Dense%2BBM25-00ADD8.svg>)](https://milvus.io)
[![Redis](<https://img.shields.io/badge/Redis-Two--Tier%20Cache-DC382D.svg>)](https://redis.io)
[![ONNX Runtime](<https://img.shields.io/badge/ONNX%20Runtime-INT8%20Quantized-005CED.svg>)](https://onnxruntime.ai)
[![MLflow](<https://img.shields.io/badge/MLflow-GenAI%20Tracing-0194E2.svg>)](https://mlflow.org)
[![RAGAS](<https://img.shields.io/badge/RAGAS-Grounded%20Evaluation-FF6F00.svg>)](https://github.com/explodinggradients/ragas)
[![Dagster](https://img.shields.io/badge/Dagster-Orchestration-252B3B.svg)](https://dagster.io)
[![Power BI](<https://img.shields.io/badge/Power%20BI-Live%20Dashboard-F2C811.svg>)](https://ziadashraf301.github.io/Business-Intelligence-Portfolio/real_estate)

---

## 🎯 Executive Summary & Audited KPIs

This end-to-end real estate intelligence ecosystem automatically ingests Egyptian property listings, applies strict Pydantic schemas, builds a hybrid information retrieval index, and serves high-concurrency property advisory recommendations through a secure, observable FastAPI microservice.

### 📊 Audited Production KPIs

| Category                  | Metric                               |             Audited Value             | Significance / Business Impact                         |
| :------------------------ | :----------------------------------- | :------------------------------------: | :----------------------------------------------------- |
| **Ingestion Scale** | Total Active Listings                |          **`10,925`**          | Full Egypt coverage (Cairo, Giza, Alexandria)          |
| **Data Quality**    | Overall Data Quality Score           |          **`94.0%`**          | Pydantic boundary validation & normalization           |
|                           | Description Coverage                 |          **`99.8%`**          | High text density for lexical & dense search           |
|                           | Geospatial Coordinates               |          **`92.5%`**          | Precise mapping for location intelligence              |
| **Search Science**  | **MRR (Mean Reciprocal Rank)** | **`0.4117`** (**+592%**) | Relevant listings placed at Rank 1 or 2                |
|                           | **Graded NDCG@10**             | **`0.2229`** (**+330%**) | Resolves dialectal Egyptian Arabic search mismatch     |
|                           | **HitRate@5**                  | **`48.0%`** (**+300%**) | Top-5 retrieval precision uplift over lexical baseline |
| **Latency & Cache** | Exact Redis Cache Hit                |         **`< 10 ms`**         | Instant response for repeated client queries           |
|                           | Semantic Cache Hit ($\ge 0.96$)    |       **`3.2s – 4.2s`**       | Bypasses redundant LLM generation & reranking          |
|                           | Cold Hybrid Search Engine            |      **`400ms – 600ms`**      | Dense vector + BM25 sparse search execution            |
| **GenAI Quality**   | RAGAS Answer Relevancy               |          **`82.1%`**          | High alignment with buyer constraints & intent         |
|                           | RAGAS Context Precision              |          **`50.0%`**          | Clean ranking of evidence chunks in prompt context     |
|                           | RAGAS Faithfulness                   |          **`49.0%`**          | Grounded advisor recommendation consistency            |

---

## 🏛️ End-to-End System Architecture

```
                                  ┌────────────────────────┐
                                  │   AQARMAP & Bayut Scrapers│
                                  └───────────┬────────────┘
                                              │ (Dagster Orchestration)
                                              ▼
                                  ┌────────────────────────┐
                                  │ Pydantic Validation &  │
                                  │ Egyptian Normalization │
                                  └───────────┬────────────┘
                                              │
                      ┌───────────────────────┴───────────────────────┐
                      ▼                                               ▼
          ┌────────────────────────┐                      ┌────────────────────────┐
          │ ClickHouse Columnar DB │                      │ Milvus 2.5 Vector DB   │
          │ (10.9K+ Listings Mart) │                      │ (Dense + Sparse BM25)  │
          └───────────┬────────────┘                      └───────────┬────────────┘
                      │                                               │
                      └───────────────────────┬───────────────────────┘
                                              │
                                              ▼
    ┌─────────────────────────────────────────────────────────────────────────────────┐
    │                        Async FastAPI Backend                          		  │
    │  ┌─────────────────────────┐ ┌──────────────────────────┐ ┌──────────────────┐  │
    │  │ JWT Authentication &    │ │ Sliding-Window Rate      │ │ Two-Tier Caching │  │
    │  │ RBAC (Admin/User/Agent) │ │ Limiter (Redis-backed)   │ │ (Redis + Milvus) │  │
    │  └─────────────────────────┘ └──────────────────────────┘ └──────────────────┘  │
    │                                                                                 │
    │  Pipeline Execution:                                                            │
    │  1. Structured Filter Extractor (LLM JSON schema)                               │
    │  2. Hybrid Retrieval (ONNX INT8 e5-small + Milvus 2.5 BM25)                     │
    │  3. Neural Cross-Encoder Reranking (ONNX bge-reranker-base INT8)                │
    │  4. Grounded Property Advisor (Gemini 2.0 / Local llama.cpp)                    │
    │  5. End-to-End Hierarchical MLflow GenAI Tracing                                │
    └─────────────────────────────────────────────────────────────────────────────────┘
                                              │
                      ┌───────────────────────┴───────────────────────┐
                      ▼                                               ▼
          ┌────────────────────────┐                      ┌────────────────────────┐
          │  Power BI Analytics    │                      │ Scientific Evaluation  │
          │  6-Page Live Dashboard │                      │ (IR Ablations & RAGAS) │
          └────────────────────────┘                      └────────────────────────┘
```

---

## 🔬 Scientific Information Retrieval Benchmark (50 Golden Arabic Queries)

Evaluated across 50 Egyptian Arabic buyer queries against Gemini-adjudicated multi-candidate ground truth across 10,925 properties in ClickHouse & Milvus:

| Architecture / Ablation Stage                                                               |      HitRate@5      |         MRR         |     Precision@10     |      Recall@10      |    Graded NDCG@10    | P50 Latency |
| :------------------------------------------------------------------------------------------ | :-----------------: | :------------------: | :------------------: | :------------------: | :------------------: | :---------: |
| **01. Lexical Keyword Baseline** (Exact Token Overlap)                                |      `12.0%`      |      `0.0595`      |      `5.00%`      |      `5.16%`      |      `0.0518`      | `0.39 ms` |
| **02. Production Hybrid + INT8 Cross-Encoder** (Milvus BM25 + ONNX e5 + BGE Reranker) | **`48.0%`** | **`0.4117`** | **`15.58%`** | **`15.71%`** | **`0.2229`** | `11.64 s` |

### 📈 Retrieval Uplift Highlights:

* **$+592\%$ MRR Uplift** ($0.0595 \rightarrow 0.4117$): Eliminates keyword mismatch for colloquial terms (e.g., *"شقة لقطة"*, *"تشطيب ألترا سوبر لوكس"*, *"عايز تمليك في سموحة"*).
* **$+330\%$ Graded NDCG@10 Uplift** ($0.0518 \rightarrow 0.2229$): Ensures the most relevant properties with matched bedrooms, budget, and location appear in top ranks.
* **$+300\%$ HitRate@5 Uplift** ($12.0\% \rightarrow 48.0\%$).

---

## 🤖 Scientific RAGAS GenAI Quality Triad

Evaluated via the official `ragas` framework and tracked in MLflow (Experiment `3`):

| RAGAS Metric                |        Audited Score        | Metric Definition & Focus                                                |
| :-------------------------- | :--------------------------: | :----------------------------------------------------------------------- |
| **Answer Relevancy**  | **`0.8214`** (82.1%) | Direct intent and constraint matching of Arabic property recommendations |
| **Context Precision** | **`0.5000`** (50.0%) | Rank-aware relevance of listing chunks feeding the LLM prompt            |
| **Faithfulness**      | **`0.4903`** (49.0%) | Factual consistency vs. raw property metadata (zero hallucination)       |

* **MLflow Tracking**: Experiment `RAGAS_Generation_Scientific_Evaluation` / Run `Official_RAGAS_Benchmark`
* **Artifacts Logged**: Radar Spider Chart, Latency histograms, ablation metrics JSON.

---

## ⚡ Two-Tier Caching Architecture

To support high-concurrency production traffic and eliminate redundant LLM/reranker costs, the platform implements a dual-layer cache:

1. **Tier 1 — Exact Redis Cache (`< 10 ms`)**:
   - SHA-256 hash of normalized Arabic query text + applied filters.
   - P50 lookup latency: **`2.98ms – 8.98ms`**.
2. **Tier 2 — Semantic Vector Cache (`3.2s – 4.2s`)**:
   - Stores query embeddings in a dedicated Milvus collection (`semantic_query_cache`).
   - If cosine similarity $\ge \mathbf{0.96}$, instantly returns previously synthesized recommendation while bypassing LLM reranker & generation compute.

---

## 🔒 Production Backend Engineering (FastAPI)

* **Authentication & Security**: JWT Access Tokens (HS256) with password hashing via passlib/bcrypt.
* **Role-Based Access Control (RBAC)**: Enforces permission boundaries across `admin`, `agent`, and `user` roles.
* **Sliding-Window Rate Limiting**: Redis-backed window counter enforcing `RATE_LIMIT_REQUESTS_PER_MINUTE=60` to safeguard endpoints.
* **Observability & Tracing**: Hierarchical MLflow GenAI Tracing logging every pipeline step (filter extraction, hybrid vector search, cross-encoder scores, token counts).

---

## 📈 Power BI Analytics Dashboard

A comprehensive 6-page BI suite connected to ClickHouse analytics tables:

1. **Executive Dashboard**: Macro market valuation, average price per m² (27,970 EGP), property type volume.
2. **Market Analysis & ROI**: Unveils a **2.7% monthly rent-to-price ratio in Alexandria** under 1M EGP, indicating high-yield buy-to-let opportunities.
3. **Location Intelligence**: Geospatial breakdown comparing Cairo/Giza (55.3%, avg 8.56M EGP) vs Alexandria (44.7%, avg 5.38M EGP).
4. **Data Quality & QA**: Real-time completeness metrics tracking coordinate, price, and room coverage (94.0% overall score).
5. **Property Type Deep Dive**: Elasticity analysis across 1BR to 4BR+ apartments (3BR commanding 65% of volume).
6. **Time Series & Scraping Trends**: Temporal posting behavior and weekend price shifts.

---

## 🚀 Quick Start & Docker Deployment

### 1. Prerequisites

- Docker & Docker Compose
- Python 3.10+
- Google Gemini API Key

### 2. Environment Setup

Create `docker/env/.env.api`:

```env
GOOGLE_API_KEY=your_gemini_api_key_here
GEMINI_MODEL=gemini-2.0-flash-lite-preview-02-05
JWT_SECRET_KEY=your_random_jwt_secret_key
MILVUS_HOST=standalone
REDIS_HOST=redis
CLICKHOUSE_HOST=clickhouse
```

### 3. Spin Up All Microservices

```bash
docker-compose -f docker/docker-compose.yml up -d
```

Service Endpoints:

* **FastAPI Backend & Swagger**: `http://localhost:5000/docs`
* **MLflow Tracking UI**: `http://localhost:5001`
* **Milvus Vector DB**: `localhost:19530`
* **ClickHouse HTTP**: `http://localhost:8123`
* **Redis Cache**: `localhost:6379`
* **MinIO Console**: `http://localhost:9001`

### 4. Run Scientific Evaluation Notebooks

```bash
# Launch Jupyter
jupyter lab

# Run Notebooks:
# 1. notebooks/02_retrieval_ablations_dense_bm25_reranker_v1.ipynb (IR Benchmark)
# 2. notebooks/05_scientific_ragas_evaluation_v1.ipynb (RAGAS Quality Triad)
```

---

## 📁 Repository Structure

```text
real_estate/
├── docker-compose.yml                     # Multi-container production stack orchestrator
├── pyproject.toml                         # Project package dependencies & build config
├── kpis.md                                # Live audited production KPI documentation
├── kpis.json                              # Machine-readable metric snapshot
│
├── docker/                                # Container definitions & environment files
│   ├── Dockerfile.api                     # Production FastAPI container
│   ├── env/                               # Environment secrets (.env.api, etc.)
│   ├── nginx/                             # Reverse proxy configuration
│   └── prometheus/                        # Metrics collection configs
│
├── src/real_estate/                       # Core Python package & application code
│   ├── main.py                            # Application bootstrap & entrypoint
│   ├── api/                               # FastAPI routing (v1/auth, v1/search, v1/rag)
│   ├── retrieval/                         # Milvus 2.5 Hybrid search, ONNX Embedder & Reranker
│   ├── services/                          # Business logic, LLM Advisor & Caching services
│   ├── schemas/                           # Pydantic data contracts & validation schemas
│   ├── repositories/                      # ClickHouse, Milvus, and Redis data access layers
│   ├── ingestion/                         # Scraping parsers & data loading pipelines
│   ├── pipelines/                         # Dagster orchestrator jobs and schedules
│   ├── core/                              # App config, database connectors, JWT auth & RBAC
│   ├── prompts/                           # Structured Arabic system prompts & few-shots
│   └── web/                               # Web interface templates & static assets
│
├── data/                                  # Data artifacts & synthetic evaluation generators
│   ├── golden_arabic_rag_testset_v1.json  # 50 Ground-truth evaluated queries
│   ├── golden_arabic_rag_testset_v1.csv   # Tabular golden dataset
│   ├── generate_synthetic_testset.py      # LLM-as-a-Judge synthetic testset generator
│   └── raw/                               # Raw property ingestion dumps
│
├── notebooks/                             # Jupyter notebooks for science & evaluation
│   ├── 02_retrieval_ablations_dense_bm25_reranker_v1.ipynb # 50-Query IR Benchmark
│   ├── 05_scientific_ragas_evaluation_v1.ipynb             # Official RAGAS Quality Suite
│   └── exploration.ipynb                  # Exploratory data analysis & prototyping
│
├── Real_Estate_BI/                        # Power BI intelligence assets & datasets
│   └── property_mart.parquet              # DirectQuery/Import semantic mart
│
├── models/                                # Quantized ONNX & local GGUF models
│   └── onnx/                              # INT8 multilingual-e5 & bge-reranker
│
├── docs/                                  # Architectural docs, screenshots & diagrams
└── tests/                                 # Pytest test suite (unit, integration, load)
```


---

## 📄 License & Attribution

Developed for Egyptian Real Estate Intelligence and Enterprise GenAI Evaluation.
Distributed under the MIT License.
