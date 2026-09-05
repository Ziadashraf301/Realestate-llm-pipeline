"""
Prometheus Application Metrics Instrumentation (Enterprise Observability).
Defines custom counters, histograms, and gauges for system throughput, latency, cache hits, and RAG execution.
"""

from prometheus_client import Counter, Histogram, Gauge

# 1. HTTP Traffic Metrics
HTTP_REQUEST_COUNT = Counter(
    "http_requests_total",
    "Total count of incoming HTTP requests",
    ["method", "endpoint", "status_code"]
)

HTTP_REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request execution latency in seconds",
    ["method", "endpoint"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
)

# 2. RAG & Vector Engine Metrics
RAG_QUERY_COUNT = Counter(
    "rag_queries_total",
    "Total count of RAG advisor queries executed",
    ["cached", "cache_tier"]
)

RAG_QUERY_LATENCY = Histogram(
    "rag_query_duration_seconds",
    "RAG pipeline execution stage latency in seconds",
    ["stage"],
    buckets=[0.01, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0]
)

# 3. Two-Tier Cache Metrics
CACHE_HIT_COUNT = Counter(
    "cache_hits_total",
    "Total count of cache hits by tier",
    ["tier"]
)

CACHE_MISS_COUNT = Counter(
    "cache_misses_total",
    "Total count of cache misses requiring live retrieval"
)

# 4. System & Model Gauges
ACTIVE_WEBSOCKET_CONNECTIONS = Gauge(
    "active_websocket_connections",
    "Current active real-time discovery sessions"
)
