# Real_Estate_Data_Pipelines/src/helpers/embedding_service/EmbeddingService.py
import requests
from typing import List
import numpy as np
import time
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
from Real_Estate_Data_Pipelines.src.logger import LoggerFactory


class EmbeddingService:
    """Handles embedding generation using Ollama - OPTIMIZED VERSION"""
    
    def __init__(self, 
                 ollama_url: str,
                 model_name: str = 'nomic-embed-text',  # Changed default to faster model
                 embedding_dim: int = 768,
                 log_dir=None):
        """
        Initialize Ollama embedding service - OPTIMIZED
        
        Args:
            ollama_url: The URL (e.g., 'http://44.202.103.44:11434')
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
        
        # Set base URL for requests
        self.base_url = base_url
        
        # Optimized settings for 8GB EC2
        self.max_workers = 8  # Parallel workers for batch processing
        self.timeout = 60
        self.request_delay = 0.05  # Small delay between parallel requests
        
        # Test connection
        self._test_connection()
        
        self.logger.info(f"✅ Connected (dimension: {self.embedding_dim})")
        self.logger.info(f"⚡ Optimized: {self.max_workers} parallel workers")
    
    def _test_connection(self):
        """Test connection to Ollama server - OPTIMIZED"""
        try:
            start_time = time.time()
            
            # Use direct HTTP request instead of OpenAI client for speed
            response = requests.get(
                f"{self.base_url}/api/tags",
                timeout=10
            )
            response.raise_for_status()
            
            # Check if model is available
            models = response.json().get('models', [])
            model_names = [m['name'] for m in models]
            
            if self.model_name not in model_names:
                self.logger.warning(f"Model {self.model_name} not found. Available: {model_names}")
                # Fallback to nomic-embed-text if available
                if 'nomic-embed-text' in model_names:
                    self.model_name = 'nomic-embed-text'
                    self.embedding_dim = 768
                    self.logger.info(f"Using fallback model: {self.model_name}")
            
            # Test embedding generation
            test_response = requests.post(
                f"{self.base_url}/api/embeddings",
                json={"model": self.model_name, "prompt": "test"},
                timeout=30
            )
            test_response.raise_for_status()
            
            data = test_response.json()
            actual_dim = len(data["embedding"])
            
            if actual_dim != self.embedding_dim:
                self.logger.warning(
                    f"⚠️ Embedding dimension mismatch! "
                    f"Expected: {self.embedding_dim}, Got: {actual_dim}"
                )
                self.embedding_dim = actual_dim
            
            elapsed = time.time() - start_time
            self.logger.info(f"✅ Ollama server responding ({elapsed:.2f}s)")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to connect to Ollama: {e}")
            raise ConnectionError(f"Cannot connect to Ollama: {e}")
    
    def _normalize_embedding(self, embedding: np.ndarray) -> np.ndarray:
        """Normalize embedding to unit length - OPTIMIZED"""
        norm = np.linalg.norm(embedding)
        if norm > 0:
            return embedding / norm
        return embedding
    
    def _encode_single_optimized(self, text: str) -> np.ndarray:
        """Optimized single text encoding for parallel processing"""
        try:
            # Clean text quickly
            if not text or not isinstance(text, str):
                text = " "
            text = text[:1000].strip()  # Limit length for speed
            
            response = requests.post(
                f"{self.base_url}/api/embeddings",
                json={
                    "model": self.model_name,
                    "prompt": text
                },
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                data = response.json()
                embedding = np.array(data["embedding"], dtype=np.float32)
                
                # Quick NaN check
                if np.isnan(embedding).any():
                    embedding = np.nan_to_num(embedding, nan=0.0)
                
                # Normalize
                embedding = self._normalize_embedding(embedding)
                
                return embedding
            else:
                # Return zero vector on failure
                return np.zeros(self.embedding_dim, dtype=np.float32)
                
        except Exception:
            # Return zero vector on error
            return np.zeros(self.embedding_dim, dtype=np.float32)
    
    def encode(self, text: str, normalize: bool = True) -> np.ndarray:
        """
        Generate embedding for text - OPTIMIZED
        
        Args:
            text: Input text
            normalize: Whether to normalize embedding
            
        Returns:
            Embedding as numpy array
        """
        try:
            response = requests.post(
                f"{self.base_url}/api/embeddings",
                json={
                    "model": self.model_name,
                    "prompt": text[:1000]  # Limit text length
                },
                timeout=self.timeout
            )
            
            if response.status_code != 200:
                raise Exception(f"HTTP {response.status_code}")
            
            data = response.json()
            embedding = np.array(data["embedding"], dtype=np.float32)
            
            # Fix NaN values
            if np.isnan(embedding).any():
                embedding = np.nan_to_num(embedding, nan=0.0)
            
            if normalize:
                embedding = self._normalize_embedding(embedding)
            
            return embedding
            
        except Exception as e:
            self.logger.error(f"Error generating embedding: {e}")
            # Return zero vector as fallback
            return np.zeros(self.embedding_dim, dtype=np.float32)
    
    def encode_batch(self, 
                     texts: List[str], 
                     normalize: bool = True,
                     batch_size: int = 100) -> List[np.ndarray]:
        """
        Generate embeddings for multiple texts - OPTIMIZED with parallel processing
        
        Args:
            texts: List of input texts
            normalize: Whether to normalize embeddings (kept for compatibility)
            batch_size: Number of texts to process in each parallel batch
            
        Returns:
            List of embeddings as numpy arrays
        """
        all_embeddings = []
        total = len(texts)
        
        self.logger.info(f"🚀 OPTIMIZED: Processing {total:,} texts")
        self.logger.info(f"⚡ Using {self.max_workers} parallel workers")
        
        start_time = time.time()
        
        # Process in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_index = {}
            for i, text in enumerate(texts):
                future = executor.submit(self._encode_single_optimized, text)
                future_to_index[future] = i
            
            # Collect results as they complete
            completed = 0
            last_log_time = time.time()
            
            # Initialize results array
            all_embeddings = [None] * total
            
            for future in concurrent.futures.as_completed(future_to_index):
                idx = future_to_index.pop(future)
                try:
                    embedding = future.result(timeout=self.timeout)
                    all_embeddings[idx] = embedding
                    completed += 1
                    
                    # Log progress every 100 texts or 5 seconds
                    current_time = time.time()
                    if completed % 100 == 0 or (current_time - last_log_time) > 5:
                        elapsed = current_time - start_time
                        texts_per_second = completed / elapsed if elapsed > 0 else 0
                        remaining = total - completed
                        
                        if texts_per_second > 0:
                            eta_seconds = remaining / texts_per_second
                            eta_minutes = eta_seconds / 60
                            
                            self.logger.info(
                                f"📊 {completed:,}/{total:,} "
                                f"({completed/total*100:.1f}%) | "
                                f"Speed: {texts_per_second:.1f} texts/s | "
                                f"ETA: {eta_minutes:.1f} min"
                            )
                        
                        last_log_time = current_time
                        
                except Exception as e:
                    self.logger.warning(f"Failed at index {idx}: {e}")
                    all_embeddings[idx] = np.zeros(self.embedding_dim, dtype=np.float32)
                    completed += 1
        
        # Ensure no None values remain
        for i in range(total):
            if all_embeddings[i] is None:
                all_embeddings[i] = np.zeros(self.embedding_dim, dtype=np.float32)
        
        total_time = time.time() - start_time
        avg_speed = total / total_time if total_time > 0 else 0
        
        self.logger.info(f"✅ COMPLETED: {total:,} embeddings")
        self.logger.info(f"⏱️ Total time: {total_time:.1f}s ({total_time/60:.1f} min)")
        self.logger.info(f"⚡ Average speed: {avg_speed:.1f} texts/s")
        
        return all_embeddings
    
    def get_dimension(self) -> int:
        """Get embedding dimension"""
        return self.embedding_dim