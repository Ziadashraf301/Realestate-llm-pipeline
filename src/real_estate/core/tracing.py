"""
Official MLflow GenAI Tracing & Observability Module.
Uses native MLflow 2.20+ Tracing APIs (mlflow.start_span & SpanType)
to produce compliant, interactive traces in the MLflow 'Traces' UI tab.
"""

import time
from typing import Any, Dict, Optional
from contextlib import contextmanager
import mlflow
from mlflow.entities import SpanType
from real_estate.core.settings import settings
from real_estate.core.logger import logger


# Token pricing per 1,000,000 tokens in USD
PRICING_RATES = {
    "llama.cpp": {"input_per_million": 0.0, "output_per_million": 0.0},  # Free local CPU
    "gemini-2.0-flash": {"input_per_million": 0.10, "output_per_million": 0.40},
    "gemini-3.1-flash-lite-preview": {"input_per_million": 0.075, "output_per_million": 0.30},
}


def calculate_cost_usd(engine: str, prompt_tokens: int, completion_tokens: int) -> float:
    """Calculates dollar cost for LLM inference based on token consumption."""
    rate = PRICING_RATES.get(engine, PRICING_RATES["llama.cpp"])
    cost = (prompt_tokens / 1_000_000.0) * rate["input_per_million"] + (
        completion_tokens / 1_000_000.0
    ) * rate["output_per_million"]
    return round(cost, 6)


class MLflowTracer:
    """
    Standard Native MLflow GenAI Tracer.
    Implements official MLflow start_span context manager with native SpanType.
    """

    _initialized = False

    @classmethod
    def initialize(cls) -> None:
        """Configures MLflow tracking URI and active experiment."""
        if not cls._initialized:
            try:
                mlflow.set_tracking_uri(settings.MLFLOW_TRACKING_URI)
                mlflow.set_experiment(settings.MLFLOW_EXPERIMENT_NAME)
                cls._initialized = True
                logger.info(
                    "mlflow_tracing_initialized",
                    tracking_uri=settings.MLFLOW_TRACKING_URI,
                    experiment=settings.MLFLOW_EXPERIMENT_NAME
                )
            except Exception as e:
                logger.warning("mlflow_tracing_initialization_skipped", error=str(e))

    @classmethod
    @contextmanager
    def span(
        cls,
        name: str,
        span_type: str = "CHAIN",
        inputs: Optional[Dict[str, Any]] = None,
        attributes: Optional[Dict[str, Any]] = None
    ):
        """
        Official MLflow start_span context manager.
        Automatically attaches to active parent span or creates a root trace.
        """
        cls.initialize()

        # Map string span types to native MLflow SpanType enum
        type_mapping = {
            "CHAIN": SpanType.CHAIN,
            "RETRIEVER": SpanType.RETRIEVER,
            "EMBEDDING": SpanType.EMBEDDING,
            "CHAT_MODEL": SpanType.CHAT_MODEL,
            "LLM": SpanType.LLM,
            "TOOL": SpanType.TOOL,
            "AGENT": SpanType.AGENT,
        }
        st = type_mapping.get(str(span_type).upper(), span_type)

        try:
            with mlflow.start_span(name=name, span_type=st) as span_obj:
                if inputs and isinstance(inputs, dict):
                    try:
                        span_obj.set_inputs(inputs)
                    except Exception:
                        pass
                if attributes and isinstance(attributes, dict):
                    for k, v in attributes.items():
                        try:
                            span_obj.set_attribute(k, v)
                        except Exception:
                            pass
                yield span_obj
        except Exception:
            # Fallback if tracing is not reachable so request never fails
            class DummySpan:
                def set_inputs(self, *a, **kw): pass
                def set_outputs(self, *a, **kw): pass
                def set_attribute(self, *a, **kw): pass
                def set_attributes(self, *a, **kw): pass
            yield DummySpan()

    @classmethod
    def flush(cls) -> None:
        """Flushes trace queue immediately to MLflow backend."""
        try:
            if hasattr(mlflow, "flush_async_logging"):
                mlflow.flush_async_logging()
            elif hasattr(mlflow, "tracing") and hasattr(mlflow.tracing, "flush_trace_async_logging"):
                mlflow.tracing.flush_trace_async_logging()
        except Exception:
            pass

    @classmethod
    def log_llm_generation(
        cls,
        engine: str,
        prompt: str,
        completion: str,
        latency_ms: float,
        prompt_tokens: Optional[int] = None,
        completion_tokens: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Logs generation metrics, token counts, and cost attribution."""
        p_tokens = prompt_tokens or max(1, len(prompt) // 4)
        c_tokens = completion_tokens or max(1, len(completion) // 4)
        total_tokens = p_tokens + c_tokens
        cost_usd = calculate_cost_usd(engine, p_tokens, c_tokens)

        metrics = {
            "engine": engine,
            "prompt_tokens": p_tokens,
            "completion_tokens": c_tokens,
            "total_tokens": total_tokens,
            "cost_usd": cost_usd,
            "latency_ms": round(latency_ms, 2)
        }

        # Log metadata directly to current active span if available
        try:
            active_span = mlflow.get_current_active_span()
            if active_span:
                active_span.set_attribute("llm_engine", engine)
                active_span.set_attribute("total_tokens", total_tokens)
                active_span.set_attribute("cost_usd", cost_usd)
        except Exception:
            pass

        logger.info(
            "llm_observability_event",
            engine=engine,
            total_tokens=total_tokens,
            cost_usd=f"${cost_usd:.6f}",
            latency_ms=round(latency_ms, 2)
        )

        return metrics

    @classmethod
    def log_vector_pipeline_run(
        cls,
        total_indexed: int,
        batch_size: int,
        duration_seconds: float,
        model_name: str = "multilingual-e5-small-int8",
        vector_db: str = "Milvus Standalone (HNSW)"
    ) -> None:
        """Logs vector embedding pipeline execution metrics to MLflow."""
        cls.initialize()
        try:
            with mlflow.start_run(run_name="pipeline_vector_indexing"):
                mlflow.log_params({
                    "model_name": model_name,
                    "vector_db": vector_db,
                    "batch_size": batch_size,
                    "dimension": 384,
                })
                mlflow.log_metrics({
                    "vectors_indexed": float(total_indexed),
                    "duration_seconds": round(duration_seconds, 2),
                    "throughput_props_per_sec": round(total_indexed / max(0.01, duration_seconds), 2),
                })
        except Exception as e:
            logger.debug("mlflow_vector_run_log_skipped", error=str(e))
