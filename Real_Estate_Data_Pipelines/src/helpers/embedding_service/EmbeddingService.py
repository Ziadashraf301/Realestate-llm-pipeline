from sentence_transformers import SentenceTransformer
from typing import List
import numpy as np
from src.logger import LoggerFactory

class EmbeddingService:
    """Handles embedding generation"""
    
    def __init__(self, model_name: str = 'all-MiniLM-L6-v2', log_dir=None):
        self.log_dir = log_dir
        self.logger = LoggerFactory.create_logger(log_dir=self.log_dir)

        self.logger.info(f"🤖 Loading embedding model: {model_name}...")
        
        self.model = SentenceTransformer(model_name)
        self.embedding_dim = self.model.get_sentence_embedding_dimension()

        self.logger.info(f"✅ Model loaded (dimension: {self.embedding_dim})")
    
    def encode(self, text: str, convert_to_numpy=True, normalize_embeddings=True) -> np.ndarray:
        """Generate embedding for text"""
        return self.model.encode(
            text, 
            convert_to_numpy=convert_to_numpy, 
            normalize_embeddings=normalize_embeddings
        )

    def encode_batch(self, texts: List[str], convert_to_numpy=True, normalize_embeddings: bool = True, 
                    batch_size: int = 32) -> List[np.ndarray]:
        """Generate embeddings for multiple texts"""
        return self.model.encode(
            texts,
            convert_to_numpy=convert_to_numpy,
            normalize_embeddings=normalize_embeddings,
            batch_size=batch_size,
            show_progress_bar=True
        )
    
    def get_dimension(self) -> int:
        """Get embedding dimension"""
        return self.embedding_dim