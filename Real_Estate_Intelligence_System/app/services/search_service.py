from Real_Estate_Data_Pipelines.src.helpers import EmbeddingService
from Real_Estate_Data_Pipelines.src.databases import Milvus_VectorDatabase
from Real_Estate_Intelligence_System.app.filters_processing.property_filters import build_filter_expr

model = None
vectordb = None

def initialize_embedding_model(cfg):
    """Initialize embedding model and Milvus vector database"""
    global model, vectordb
    
    # Initialize embedding service
    model = EmbeddingService(cfg.OLLAMA_URL, cfg.EMBEDDING_MODEL, log_dir=cfg.LOG_DIR)
    
    # Initialize and connect to Milvus
    vectordb = Milvus_VectorDatabase(
        cfg.LOG_DIR, 
        cfg.MILVUS_HOST, 
        cfg.MILVUS_PORT,
        cfg.MILVUS_COLLECTION_NAME, 
        embedding_dim=cfg.EMBEDDING_DIM,
        embedding_model=cfg.EMBEDDING_MODEL
    )
    
    vectordb.connect()
    vectordb.load_collection()


def search_properties(data):
    """
    Search properties using vector similarity.
    
    Args:
        data: Dictionary containing:
            - query: Search query string
            - n_results: Number of results to return (default: 10)
            - Additional filter parameters for build_filter_expr
    
    Returns:
        List of formatted property results with similarity scores
    """
    query = data.get('query', '')
    n_results = data.get('n_results', 10)
    
    # Generate query embedding
    query_embedding = model.encode(
        query, 
        convert_to_numpy=True, 
        normalize_embeddings=True
    )
    
    # Build filter expression from data
    filter_expr = build_filter_expr(data)
    
    # Search using vectordb method
    results = vectordb.search_vectors(
        query_embedding=query_embedding.tolist(),
        filter_expr=filter_expr,
        limit=n_results,
        nprobe=64
    )
    
    return results


def get_collection_count():
    """Get the total number of properties in the collection"""
    return vectordb.get_collection_stats()