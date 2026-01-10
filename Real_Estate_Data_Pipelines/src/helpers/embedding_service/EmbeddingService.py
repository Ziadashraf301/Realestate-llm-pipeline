from openai import OpenAI
from typing import List, Union, Optional
import numpy as np
from Real_Estate_Data_Pipelines.src.logger import LoggerFactory


class EmbeddingService:
    """Handles embedding generation using Ollama with OpenAI-compatible API"""
    
    def __init__(self, 
                 ollama_url: str,
                 model_name: str = 'jeffh/intfloat-multilingual-e5-small:f32',
                 embedding_dim: int = 384,  
                 log_dir=None,
                 default_mode: str = 'passage'):  # 'query' or 'passage'
        """
        Initialize Ollama embedding service
        
        Args:
            ollama_url: The URL (e.g., 'https://abc123.ngrok.io' or 'http://localhost:11434')
            model_name: Ollama embedding model name (default: E5-small)
            embedding_dim: Expected embedding dimension (384 for E5-small)
            log_dir: Directory for logs
            default_mode: Default prefix mode - 'passage' for ETL, 'query' for inference
        """
        self.log_dir = log_dir
        self.logger = LoggerFactory.create_logger(log_dir=self.log_dir)
        
        # Validate mode
        if default_mode not in ['query', 'passage']:
            raise ValueError(f"default_mode must be 'query' or 'passage', got {default_mode}")
        
        # Ensure URL doesn't have trailing slash
        base_url = ollama_url.rstrip('/')
        self.model_name = model_name
        self.embedding_dim = embedding_dim
        self.default_mode = default_mode
        
        self.logger.info(f"🤖 Connecting to Ollama at: {base_url}")
        self.logger.info(f"📦 Using model: {model_name}")
        self.logger.info(f"🔤 Default mode: {default_mode} (prefix: '{default_mode}: ')")
        
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
            # Test with appropriate prefix for E5 model
            test_text = f"{self.default_mode}: test connection"
            test_response = self.client.embeddings.create(
                model=self.model_name,
                input=[test_text]
            )
            actual_dim = len(test_response.data[0].embedding)
            
            if actual_dim != self.embedding_dim:
                self.logger.warning(
                    f"⚠️ Embedding dimension mismatch! "
                    f"Expected: {self.embedding_dim}, Got: {actual_dim}. "
                    f"Updating to actual dimension."
                )
                self.embedding_dim = actual_dim
            
            self.logger.info(f"✅ Ollama server is reachable (actual dim: {actual_dim})")
        except Exception as e:
            self.logger.error(f"❌ Failed to connect to Ollama: {e}")
            raise ConnectionError(f"Cannot connect to Ollama: {e}")
    
    def _add_prefix(self, 
                    text: Union[str, List[str]], 
                    prefix_type: Optional[str] = None) -> Union[str, List[str]]:
        """
        Add appropriate prefix for E5 models
        
        Args:
            text: Input text or list of texts
            prefix_type: 'query' or 'passage'. If None, uses self.default_mode
        
        Returns:
            Text with prefix added
        """
        if prefix_type is None:
            prefix_type = self.default_mode
        elif prefix_type not in ['query', 'passage']:
            raise ValueError(f"prefix_type must be 'query' or 'passage', got {prefix_type}")
        
        prefix = f"{prefix_type}: "
        
        if isinstance(text, list):
            return [f"{prefix}{t}" if not t.startswith(('query: ', 'passage: ')) else t 
                    for t in text]
        else:
            if text.startswith(('query: ', 'passage: ')):
                return text
            return f"{prefix}{text}"
    
    def _normalize_embedding(self, embedding: np.ndarray) -> np.ndarray:
        """Normalize embedding to unit length"""
        norm = np.linalg.norm(embedding)
        if norm > 0:
            return embedding / norm
        return embedding
    
    def encode(self, 
               text: str, 
               normalize: bool = True,
               prefix_type: Optional[str] = None) -> np.ndarray:
        """
        Generate embedding for text
        
        Args:
            text: Input text
            normalize: Whether to normalize embedding
            prefix_type: Override default prefix ('query' or 'passage')
            
        Returns:
            Embedding as numpy array
        """
        try:
            # Add appropriate prefix for E5 model
            prefixed_text = self._add_prefix(text, prefix_type)
            
            response = self.client.embeddings.create(
                model=self.model_name,
                input=[prefixed_text]
            )
            
            embedding = np.array(response.data[0].embedding, dtype=np.float32)
            
            if normalize:
                embedding = self._normalize_embedding(embedding)
            
            return embedding
            
        except Exception as e:
            self.logger.error(f"Error generating embedding for text '{text[:50]}...': {e}")
            raise
    
    def encode_batch(self, 
                     texts: List[str], 
                     normalize: bool = True,
                     batch_size: int = 16,
                     prefix_type: Optional[str] = None) -> List[np.ndarray]:
        """
        Generate embeddings for multiple texts with proper batching
        
        Args:
            texts: List of input texts
            normalize: Whether to normalize embeddings
            batch_size: Number of texts to send in each API call
            prefix_type: Override default prefix ('query' or 'passage')
            
        Returns:
            List of embeddings as numpy arrays
        """
        if not texts:
            return []
            
        all_embeddings = []
        total = len(texts)
        
        self.logger.info(f"Processing {total} texts in batches of {batch_size}...")
        
        # Add prefixes to all texts first
        prefixed_texts = self._add_prefix(texts, prefix_type)
        
        # Process in batches
        for i in range(0, total, batch_size):
            batch = prefixed_texts[i:i + batch_size]
            original_batch = texts[i:i + batch_size]  # Keep original for error handling
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
                    for item in response.data
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
                for text in original_batch:
                    try:
                        embedding = self.encode(text, normalize=normalize, prefix_type=prefix_type)
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
    
    def switch_mode(self, mode: str):
        """
        Switch between query and passage mode
        
        Args:
            mode: 'query' or 'passage'
        """
        if mode not in ['query', 'passage']:
            raise ValueError(f"Mode must be 'query' or 'passage', got {mode}")
        
        self.default_mode = mode
        self.logger.info(f"Switched to {mode} mode (prefix: '{mode}: ')")
    
    def create_query_embedder(self):
        """Create a new instance configured for query mode (convenience method)"""
        return EmbeddingService(
            ollama_url=self.client.base_url.replace('/v1', ''),
            model_name=self.model_name,
            embedding_dim=self.embedding_dim,
            log_dir=self.log_dir,
            default_mode='query'
        )
    
    def create_passage_embedder(self):
        """Create a new instance configured for passage mode (convenience method)"""
        return EmbeddingService(
            ollama_url=self.client.base_url.replace('/v1', ''),
            model_name=self.model_name,
            embedding_dim=self.embedding_dim,
            log_dir=self.log_dir,
            default_mode='passage'
        )