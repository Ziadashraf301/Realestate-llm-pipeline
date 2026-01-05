# Real_Estate_Data_Pipelines/src/helpers/embedding_service/EmbeddingService.py
import requests
from typing import List
import numpy as np
import time
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
from Real_Estate_Data_Pipelines.src.logger import LoggerFactory


class EmbeddingService:
    """Handles embedding generation using Ollama - OPTIMIZED VERSION with batch logic"""
    
    def __init__(self, 
                 ollama_url: str,
                 model_name: str = 'nomic-embed-text',
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
        self.max_workers = 2  # Parallel workers for batch processing
        self.timeout = 60
        
        # Test connection
        self._test_connection()
        
        self.logger.info(f"✅ Connected (dimension: {self.embedding_dim})")
        self.logger.info(f"⚡ Optimized: {self.max_workers} parallel workers")
    
    def _test_connection(self):
        """Test connection to Ollama server - OPTIMIZED"""
        try:
            start_time = time.time()
            
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
        """Normalize embedding to unit length"""
        norm = np.linalg.norm(embedding)
        if norm > 0:
            return embedding / norm
        return embedding
    
    def _encode_single(self, text: str, normalize: bool = True) -> np.ndarray:
        """Single text encoding for parallel processing"""
        try:
            # Clean text
            if not text or not isinstance(text, str):
                text = " "
            text = text[:1000].strip()  # Limit length
            
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
                
                # Fix NaN values
                if np.isnan(embedding).any():
                    embedding = np.nan_to_num(embedding, nan=0.0)
                
                if normalize:
                    embedding = self._normalize_embedding(embedding)
                
                return embedding
            else:
                return np.zeros(self.embedding_dim, dtype=np.float32)
                
        except Exception:
            return np.zeros(self.embedding_dim, dtype=np.float32)
    
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
            response = requests.post(
                f"{self.base_url}/api/embeddings",
                json={
                    "model": self.model_name,
                    "prompt": text[:1000]
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
            return np.zeros(self.embedding_dim, dtype=np.float32)
    
    def encode_batch(self, 
                     texts: List[str], 
                     normalize: bool = True,
                     batch_size: int = 100) -> List[np.ndarray]:
        """
        Generate embeddings for multiple texts with batch logic and parallel processing
        
        Args:
            texts: List of input texts
            normalize: Whether to normalize embeddings
            batch_size: Number of texts to send in each batch
            
        Returns:
            List of embeddings as numpy arrays
        """
        all_embeddings = []
        total = len(texts)
        
        self.logger.info(f"🚀 Processing {total:,} texts in batches of {batch_size}")
        self.logger.info(f"⚡ Using {self.max_workers} parallel workers")
        
        start_time = time.time()
        total_batches = (total + batch_size - 1) // batch_size
        
        # Process each batch in parallel internally
        for batch_num in range(total_batches):
            batch_start = batch_num * batch_size
            batch_end = min(batch_start + batch_size, total)
            batch = texts[batch_start:batch_end]
            actual_batch_size = len(batch)
            
            self.logger.info(f"🔄 Processing batch {batch_num + 1}/{total_batches} ({actual_batch_size} texts)")
            batch_start_time = time.time()
            
            # Process this batch in parallel
            batch_embeddings = self._process_batch_parallel(batch, normalize)
            
            batch_time = time.time() - batch_start_time
            batch_speed = actual_batch_size / batch_time if batch_time > 0 else 0
            
            self.logger.info(f"   ✓ Batch {batch_num + 1} completed in {batch_time:.1f}s ({batch_speed:.1f} texts/s)")
            
            all_embeddings.extend(batch_embeddings)
            
            # Calculate overall progress
            processed = batch_end
            elapsed = time.time() - start_time
            overall_speed = processed / elapsed if elapsed > 0 else 0
            
            if overall_speed > 0:
                remaining = total - processed
                eta_seconds = remaining / overall_speed
                eta_minutes = eta_seconds / 60
                
                self.logger.info(
                    f"📊 Overall: {processed:,}/{total:,} "
                    f"({processed/total*100:.1f}%) | "
                    f"Speed: {overall_speed:.1f} texts/s | "
                    f"ETA: {eta_minutes:.1f} min"
                )
        
        total_time = time.time() - start_time
        avg_speed = total / total_time if total_time > 0 else 0
        
        self.logger.info(f"✅ COMPLETED: {total:,} embeddings")
        self.logger.info(f"⏱️ Total time: {total_time:.1f}s ({total_time/60:.1f} min)")
        self.logger.info(f"⚡ Average speed: {avg_speed:.1f} texts/s")
        
        return all_embeddings
    
    def _process_batch_parallel(self, batch: List[str], normalize: bool = True) -> List[np.ndarray]:
        """
        Process a single batch in parallel
        
        Args:
            batch: List of texts in this batch
            normalize: Whether to normalize
            
        Returns:
            List of embeddings for this batch
        """
        batch_size = len(batch)
        batch_embeddings = [None] * batch_size
        
        # Process batch texts in parallel
        with ThreadPoolExecutor(max_workers=min(self.max_workers, batch_size)) as executor:
            future_to_index = {}
            
            # Submit all texts in this batch
            for i, text in enumerate(batch):
                future = executor.submit(self._encode_single, text, normalize)
                future_to_index[future] = i
            
            # Collect results as they complete
            for future in as_completed(future_to_index):
                idx = future_to_index.pop(future)
                try:
                    embedding = future.result(timeout=self.timeout)
                    batch_embeddings[idx] = embedding
                except Exception as e:
                    self.logger.debug(f"Failed in batch at index {idx}: {e}")
                    batch_embeddings[idx] = np.zeros(self.embedding_dim, dtype=np.float32)
        
        # Ensure no None values
        for i in range(batch_size):
            if batch_embeddings[i] is None:
                batch_embeddings[i] = np.zeros(self.embedding_dim, dtype=np.float32)
        
        return batch_embeddings
    
    def get_dimension(self) -> int:
        """Get embedding dimension"""
        return self.embedding_dim