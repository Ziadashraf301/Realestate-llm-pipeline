"""
ONNX Runtime Cross-Encoder Re-ranker (Production MLOps).
Quantized INT8 inference for BAAI/bge-reranker-base.
Loads versioned model artifacts from local cache or MLflow Model Registry.
Strict production guarantees: ZERO PyTorch, ZERO transformers imports, ZERO mock fallbacks.
"""

from typing import Any, Dict, List
import numpy as np
import onnxruntime as ort
from tokenizers import Tokenizer

from real_estate.core.model_registry import resolve_model_artifacts
from real_estate.core.tracing import MLflowTracer
from real_estate.core.logger import logger


class OnnxCrossEncoderService:
    """Singleton ONNX Runtime Cross-Encoder for deep semantic re-ranking on CPU."""

    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        # 1. Resolve artifacts from local storage or MLflow Model Registry
        model_path, tokenizer_path = resolve_model_artifacts("reranker")
        logger.info("initializing_onnx_cross_encoder_service", model_path=str(model_path))

        # 2. Configure ONNX Runtime session
        opts = ort.SessionOptions()
        opts.intra_op_num_threads = 4
        opts.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL

        self.session = ort.InferenceSession(
            str(model_path), opts, providers=["CPUExecutionProvider"]
        )

        # 3. Load Rust-backed tokenizer directly (Zero transformers dependency)
        self.tokenizer = Tokenizer.from_file(str(tokenizer_path))
        self.tokenizer.enable_truncation(max_length=192)
        # Dynamic padding to max length in current batch (padded to multiple of 8 for AVX2/AVX-512 SIMD vectorization)
        self.tokenizer.enable_padding(pad_to_multiple_of=8)

        logger.info("onnx_reranker_loaded_successfully", provider="CPUExecutionProvider", max_length=192)

    def _tokenize_pairs(self, pairs: List[List[str]]) -> dict:
        """Tokenizes (query, passage) pairs in parallel Rust C-threads with dynamic batch padding."""
        encodings = self.tokenizer.encode_batch(pairs)
        input_ids = [enc.ids for enc in encodings]
        attention_masks = [enc.attention_mask for enc in encodings]
        token_type_ids = [enc.type_ids for enc in encodings]

        return {
            "input_ids": np.array(input_ids, dtype=np.int64),
            "attention_mask": np.array(attention_masks, dtype=np.int64),
            "token_type_ids": np.array(token_type_ids, dtype=np.int64),
        }

    def rerank(
        self,
        query: str,
        candidates: List[Dict[str, Any]],
        top_n: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Scores (query, document) pairs with cross-attention and returns Top-N candidates.
        Optimized for sub-second CPU latency with a focused candidate window.
        """
        if not candidates:
            return []

        # Focus deep cross-encoder re-ranking on the top-15 hybrid retrieval candidates
        eval_candidates = candidates[:15]

        with MLflowTracer.span(
            "cross_encoder_rerank",
            span_type="RETRIEVER",
            inputs={"query": query, "candidate_count": len(eval_candidates), "top_n": top_n}
        ) as rerank_span:
            pairs = []
            for c in eval_candidates:
                price_val = float(c.get("price_egp") or 0.0)
                price_str = f"{price_val:,.0f} جنيه" if price_val > 0 else "السعر عند الطلب"
                beds = f"{c.get('bedrooms')} غرف" if c.get('bedrooms') is not None else ""
                baths = f"{c.get('bathrooms')} حمام" if c.get('bathrooms') is not None else ""
                area = f"{c.get('area_sqm')} م²" if c.get('area_sqm') is not None else ""
                desc = (c.get('description') or c.get('text', ''))[:300]

                doc_text = (
                    f"العقار: {c.get('title', '')} | "
                    f"النوع: {c.get('property_type', '')} {c.get('listing_type', '')} | "
                    f"الموقع: {c.get('location', '')} {c.get('city', '')} {c.get('district', '')} | "
                    f"السعر: {price_str} | "
                    f"المواصفات: {beds} {baths} {area} | "
                    f"التفاصيل: {desc}"
                )
                pairs.append([query, doc_text])

            inputs = self._tokenize_pairs(pairs)

            # Pass only what the model computation graph expects
            expected_inputs = {inp.name for inp in self.session.get_inputs()}
            onnx_inputs = {k: v for k, v in inputs.items() if k in expected_inputs}

            outputs = self.session.run(None, onnx_inputs)
            scores = np.asarray(outputs[0]).reshape(-1)

            # Sigmoid activation: convert logits to [0, 1] relevance probability
            sigmoid_scores = 1.0 / (1.0 + np.exp(-scores))

            for i, score in enumerate(sigmoid_scores):
                eval_candidates[i]["rerank_score"] = float(score)

            sorted_candidates = sorted(
                eval_candidates, key=lambda x: x.get("rerank_score", 0.0), reverse=True
            )
            top_results = sorted_candidates[:top_n]

            top_scores = [round(c.get("rerank_score", 0.0), 3) for c in top_results]
            rerank_span.set_outputs({
                "candidates_in": len(eval_candidates),
                "top_n_out": len(top_results),
                "top_scores": top_scores
            })
            logger.info(
                "cross_encoder_rerank_complete",
                candidates_in=len(candidates),
                top_n_out=len(top_results),
                top_scores=top_scores[:3]
            )
            return top_results


# Export alias
OnnxCrossEncoder = OnnxCrossEncoderService
