"""
ONNX Runtime Dense Embedding Engine (Production MLOps).
Uses INT8 quantized multilingual-e5-small with pure NumPy mean pooling and L2 normalization.
Loads versioned model artifacts from local cache or MLflow Model Registry.
Strict production guarantees: ZERO PyTorch, ZERO transformers imports, ZERO mock fallbacks.
"""

from typing import cast
import numpy as np
import onnxruntime as ort
from tokenizers import Tokenizer

from real_estate.core.model_registry import resolve_model_artifacts

from real_estate.core.logger import logger


class OnnxEmbeddingService:
    """
    Singleton ONNX Runtime dense embedding service running on CPU.
    Tokenization uses the pure Rust `tokenizers` library (zero PyTorch, zero transformers).
    Inference is pure C++ ONNX Runtime with Intra-Op multi-threading.
    """

    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.dagster_logger = kwargs.get("logger", None)
            cls._instance._initialize()
        return cls._instance

    def _log(self, level: str, msg: str, **kwargs) -> None:
        """Emits to Dagster context.log (if provided) and to standard structlog."""
        extra_str = " | ".join(f"{k}={v}" for k, v in kwargs.items())
        display_msg = f"{msg} ({extra_str})" if extra_str else msg
        if self.dagster_logger:
            try:
                fn = getattr(self.dagster_logger, level, None) or self.dagster_logger.info
                fn(display_msg)
            except Exception:
                pass
        struct_func = getattr(logger, level, None) or logger.info
        struct_func(msg, **kwargs)

    def _initialize(self):
        # 1. Resolve artifacts from local storage or MLflow Model Registry
        model_path, tokenizer_path = resolve_model_artifacts("embedding")
        self._log("info", "initializing_onnx_embedding_service", model_path=str(model_path))

        # 2. Configure ONNX Runtime session for high-throughput CPU inference
        opts = ort.SessionOptions()
        opts.intra_op_num_threads = 4
        opts.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL

        self.session = ort.InferenceSession(
            str(model_path), opts, providers=["CPUExecutionProvider"]
        )

        # 3. Load Rust-backed tokenizer (Zero transformers dependency)
        self.tokenizer = Tokenizer.from_file(str(tokenizer_path))
        self.tokenizer.enable_padding(direction="right", pad_id=0, pad_token="[PAD]")
        self.tokenizer.enable_truncation(max_length=256)

        self._log("info", "onnx_embedding_engine_ready", dimension=384, provider="CPUExecutionProvider")

    def _tokenize(self, text: str) -> dict:
        """Tokenizes text into numpy arrays directly via Rust tokenizers."""
        enc = self.tokenizer.encode(text)
        return {
            "input_ids": np.array([enc.ids], dtype=np.int64),
            "attention_mask": np.array([enc.attention_mask], dtype=np.int64),
            "token_type_ids": np.zeros((1, len(enc.ids)), dtype=np.int64),
        }

    def _tokenize_batch(self, texts: list[str]) -> dict:
        """Tokenizes a batch of texts into 2D numpy arrays directly via Rust tokenizers with dynamic length."""
        encodings = self.tokenizer.encode_batch(texts)
        input_ids = [enc.ids for enc in encodings]
        attention_mask = [enc.attention_mask for enc in encodings]
        token_type_ids = [enc.type_ids for enc in encodings]
        return {
            "input_ids": np.array(input_ids, dtype=np.int64),
            "attention_mask": np.array(attention_mask, dtype=np.int64),
            "token_type_ids": np.array(token_type_ids, dtype=np.int64),
        }

    def encode(self, text: str, is_query: bool = True) -> np.ndarray:
        """
        Encodes single text into a normalized 384-dimensional dense vector.
        """
        res = self.encode_batch([text], is_query=is_query)
        return res[0]

    def encode_batch(self, texts: list[str], is_query: bool = False, chunk_size: int = 64) -> list[np.ndarray]:
        """
        Encodes a list of texts into normalized 384-dimensional dense vectors using micro-chunks.
        Applies mean pooling and L2 unit normalization in seconds on CPU.
        """
        if not texts:
            return []

        all_embeddings: list[np.ndarray] = []
        prefix = "query: " if is_query else "passage: "
        prefixed_texts = [f"{prefix}{t}" for t in texts]

        for i in range(0, len(prefixed_texts), chunk_size):
            chunk = prefixed_texts[i : i + chunk_size]
            inputs = self._tokenize_batch(chunk)

            # Filter inputs to only what the ONNX computation graph expects
            expected_names = [inp.name for inp in self.session.get_inputs()]
            onnx_inputs = {k: v for k, v in inputs.items() if k in expected_names}

            outputs = self.session.run(None, onnx_inputs)

            # Mean pooling across token dimension weighted by attention mask
            token_embeddings = outputs[0]  # Shape: (batch_size, seq_len, 384)
            mask = np.expand_dims(inputs["attention_mask"], -1)
            pooled = np.sum(token_embeddings * mask, axis=1) / np.clip(
                mask.sum(axis=1), a_min=1e-9, a_max=None
            )

            # L2 unit normalization: ||v|| = 1
            norm = np.linalg.norm(pooled, axis=1, keepdims=True)
            normalized_vecs = pooled / np.clip(norm, a_min=1e-9, a_max=None)
            all_embeddings.extend([v.astype(np.float32) for v in normalized_vecs])

        return all_embeddings
