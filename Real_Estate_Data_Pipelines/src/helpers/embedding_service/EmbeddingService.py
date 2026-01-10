import numpy as np
from typing import List
from sentence_transformers import SentenceTransformer
from Real_Estate_Data_Pipelines.src.logger import LoggerFactory

class EmbeddingService:
    """Handles embedding generation using Hugging Face SentenceTransformers"""

    def __init__(self, model_name: str = 'intfloat/multilingual-e5-small', log_dir=None):
        self.log_dir = log_dir
        self.logger = LoggerFactory.create_logger(log_dir=self.log_dir)
        self.logger.info(f"🤖 Loading Hugging Face model: {model_name}...")

        # Load the model from Hugging Face
        self.model = SentenceTransformer(model_name)
        self.embedding_dim = self.model.get_sentence_embedding_dimension()

        self.logger.info(f"✅ Model loaded (dimension: {self.embedding_dim})")

    def encode(self, text: str, is_query: bool = True, normalize: bool = True) -> np.ndarray:
        """
        Generate embedding for a single text.
        E5 requires 'query: ' for search or 'passage: ' for document storage.
        """
        prefix = "query: " if is_query else "passage: "
        prefixed_text = f"{prefix}{text}"
        
        return self.model.encode(
            prefixed_text,
            convert_to_numpy=True,
            normalize_embeddings=normalize
        )

    def encode_batch(self, texts: List[str], is_query: bool = False, 
                     normalize: bool = True, batch_size: int = 16) -> List[np.ndarray]:
        """
        Generate embeddings for multiple texts.
        Defaults to 'passage: ' as batching is usually for document indexing.
        """
        prefix = "query: " if is_query else "passage: "
        prefixed_texts = [f"{prefix}{t}" for t in texts]
        
        return self.model.encode(
            prefixed_texts,
            convert_to_numpy=True,
            normalize_embeddings=normalize,
            batch_size=batch_size,
            show_progress_bar=True
        )

    def get_dimension(self) -> int:
        """Get embedding dimension (384 for multilingual-e5-small)"""
        return self.embedding_dim