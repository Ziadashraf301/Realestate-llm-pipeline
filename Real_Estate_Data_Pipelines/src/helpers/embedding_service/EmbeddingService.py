from openai import OpenAI
from typing import List
import numpy as np
from Real_Estate_Data_Pipelines.src.logger import LoggerFactory


class EmbeddingService:
    """Handles embedding generation using Ollama with OpenAI-compatible API"""
    
    def __init__(self, 
                 ollama_url: str,
                 model_name: str = 'bge-m3:latest',
                 embedding_dim: int = 1024,
                 log_dir=None):
        """
        Initialize Ollama embedding service
        
        Args:
            ollama_url: The URL (e.g., 'https://abc123.ngrok.io')
            model_name: Ollama embedding model name
            embedding_dim: Expected embedding dimension
            log_dir: Directory for logs
        """
        self.log_dir = log_dir
        self.logger = LoggerFactory.create_logger(log_dir=self.log_dir)
        
        # Ensure URL doesn't have trailing slash
        base_url = ollama_url.rstrip('/')
        self.model_name = model_name
        self.embedding_dim = embedding_dim
        
        self.logger.info(f"🤖 Connecting to Ollama at: {base_url}")
        self.logger.info(f"📦 Using model: {model_name}")
        
        # Initialize OpenAI client pointing to Ollama
        self.client = OpenAI(
            base_url=f"{base_url}/v1",
            api_key="ollama"
        )
        
        # Test connection
        self._test_connection()
        
        self.logger.info(f"✅ Connected (dimension: {self.embedding_dim})")
    
    def _test_connection(self):
        """Test connection to Ollama server"""
        try:
            # Try to generate a test embedding
            test_response = self.client.embeddings.create(
                model=self.model_name,
                input="test"
            )
            actual_dim = len(test_response.data[0].embedding)
            
            if actual_dim != self.embedding_dim:
                self.logger.warning(
                    f"⚠️ Embedding dimension mismatch! "
                    f"Expected: {self.embedding_dim}, Got: {actual_dim}"
                )
                self.embedding_dim = actual_dim
            
            self.logger.info("✅ Ollama server is reachable and responding")
        except Exception as e:
            self.logger.error(f"❌ Failed to connect to Ollama: {e}")
            raise ConnectionError(f"Cannot connect to Ollama: {e}")
    
    def _normalize_embedding(self, embedding: np.ndarray) -> np.ndarray:
        """Normalize embedding to unit length"""
        norm = np.linalg.norm(embedding)
        if norm > 0:
            return embedding / norm
        return embedding
    
    def encode(self, text: str, normalize: bool = True) -> np.ndarray:
        """
        Generate embedding for text
        
        Args:
            text: Input text
            normalize: Whether to normalize embedding
            
        Returns:
            Embedding as numpy array
        """
        try:
            response = self.client.embeddings.create(
                model=self.model_name,
                input=text
            )
            
            embedding = np.array(response.data[0].embedding, dtype=np.float32)
            
            if normalize:
                embedding = self._normalize_embedding(embedding)
            
            return embedding
            
        except Exception as e:
            self.logger.error(f"Error generating embedding for text: {e}")
            raise
    
    def encode_batch(self, 
                     texts: List[str], 
                     normalize: bool = True,
                     batch_size: int = 16) -> List[np.ndarray]:
        """
        Generate embeddings for multiple texts with proper batching
        
        Args:
            texts: List of input texts
            normalize: Whether to normalize embeddings
            batch_size: Number of texts to send in each API call
            
        Returns:
            List of embeddings as numpy arrays
        """
        all_embeddings = []
        total = len(texts)
        
        self.logger.info(f"Processing {total} texts in batches of {batch_size}...")
        
        # Process in batches
        for i in range(0, total, batch_size):
            batch = texts[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total + batch_size - 1) // batch_size
            
            self.logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} texts)")
            
            try:
                # Send batch to Ollama
                response = self.client.embeddings.create(
                    model=self.model_name,
                    input=batch
                )
                
                # Extract embeddings from response
                batch_embeddings = [
                    np.array(item.embedding, dtype=np.float32) 
                    for item in response
                ]
                
                # Normalize if requested
                if normalize:
                    batch_embeddings = [
                        self._normalize_embedding(emb) 
                        for emb in batch_embeddings
                    ]
                
                all_embeddings.extend(batch_embeddings)
                
            except Exception as e:
                self.logger.error(f"Error processing batch {batch_num}: {e}")
                # Fallback to individual processing for this batch
                self.logger.warning("Falling back to individual processing for failed batch")
                for text in batch:
                    try:
                        embedding = self.encode(text, normalize=normalize)
                        all_embeddings.append(embedding)
                    except Exception as text_error:
                        self.logger.error(f"Failed to process text: {text_error}")
                        # Add zero embedding as placeholder
                        all_embeddings.append(np.zeros(self.embedding_dim, dtype=np.float32))
        
        self.logger.info(f"✅ Completed {len(all_embeddings)} embeddings")
        return all_embeddings
    
    def get_dimension(self) -> int:
        """Get embedding dimension"""
        return self.embedding_dim