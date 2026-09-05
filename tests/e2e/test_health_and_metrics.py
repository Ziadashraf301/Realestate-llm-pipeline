"""
Unit & Integration Tests for System Health, Probing, and Prometheus Metrics.
"""

def test_root_health_endpoint(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "healthy"
    assert "environment" in data
    assert "llm_engine" in data
    assert "vector_db" in data
    assert "caching" in data


def test_api_v1_health_endpoint(client):
    resp = client.get("/api/v1/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "healthy"


def test_system_stats_endpoint(client):
    resp = client.get("/stats")
    assert resp.status_code == 200
    data = resp.json()
    assert data["success"] is True
    assert "total_properties" in data
    assert "cache_ttl_hours" in data


def test_prometheus_metrics_endpoint(client):
    resp = client.get("/metrics")
    assert resp.status_code == 200
    # Prometheus format is plain text exposition format
    text = resp.text
    assert "http_requests_total" in text or "process_cpu_seconds_total" in text or len(text) > 0
