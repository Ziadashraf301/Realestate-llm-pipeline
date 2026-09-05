"""
Enterprise ASGI Middleware Layer (SOLID: Single Responsibility).
Handles request correlation ID tracing and execution latency instrumentation.
"""

import time
import uuid
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from real_estate.core.metrics import HTTP_REQUEST_COUNT, HTTP_REQUEST_LATENCY


class TimingAndCorrelationMiddleware(BaseHTTPMiddleware):
    """Instruments every incoming HTTP request with correlation tracking, processing time, and Prometheus metrics."""

    async def dispatch(self, request: Request, call_next) -> Response:
        req_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
        start_time = time.perf_counter()

        response = await call_next(request)

        process_time_sec = time.perf_counter() - start_time
        process_time_ms = process_time_sec * 1000

        # Prometheus Metrics Instrumentation
        endpoint = request.url.path
        HTTP_REQUEST_COUNT.labels(method=request.method, endpoint=endpoint, status_code=str(response.status_code)).inc()
        HTTP_REQUEST_LATENCY.labels(method=request.method, endpoint=endpoint).observe(process_time_sec)

        response.headers["X-Process-Time-ms"] = f"{process_time_ms:.2f}"
        response.headers["X-Request-ID"] = req_id
        return response
