from Real_Estate_Data_Pipelines.src.helpers import EmbeddingService
from Real_Estate_Data_Pipelines.src.databases import Milvus_VectorDatabase
from app.filters_processing.property_filters import build_filter_expr
from pymilvus import Collection

model = None
collection = None

def initialize_embedding_model(cfg):
    global model, collection
    model = EmbeddingService(cfg.EMBEDDING_MODEL, log_dir=cfg.LOG_DIR)
    vectordb = Milvus_VectorDatabase(cfg.LOG_DIR, cfg.MILVUS_HOST, cfg.MILVUS_PORT,
                 cfg.MILVUS_COLLECTION_NAME, embedding_dim=cfg.EMBEDDING_DIM)
    
    vectordb.connect()
    collection = Collection(cfg.MILVUS_COLLECTION_NAME)

    try:
        if collection.is_loaded():
            vectordb.logger.info("✅ Collection is already loaded.")
    except Exception as e:
        vectordb.logger.error(f"❌ Error checking collection status: {e}")

def search_properties(data):
    query = data.get('query', '')
    n_results = data.get('n_results', 10)

    query_embedding = model.encode(query, convert_to_numpy=True, normalize_embeddings=True)
    filter_expr = build_filter_expr(data)

    search_params = {"metric_type": "COSINE", "params": {"nprobe": 64}}
    results = collection.search(
        data=[query_embedding.tolist()],
        anns_field="embedding",
        param=search_params,
        limit=n_results,
        expr=filter_expr,
        output_fields=[
            "property_id", "title", "location", "property_type",
            "listing_type", "price_egp", "bedrooms", "bathrooms",
            "area_sqm", "url", "text"
        ]
    )

    formatted_results = []
    for hit in results[0]:
        entity = hit.entity
        similarity = max(0, 1 - hit.distance)
        formatted_results.append({
            'property_id': entity.get('property_id'),
            'title': entity.get('title'),
            'location': entity.get('location'),
            'property_type': entity.get('property_type'),
            'listing_type': entity.get('listing_type'),
            'price_egp': entity.get('price_egp'),
            'bedrooms': entity.get('bedrooms'),
            'bathrooms': entity.get('bathrooms'),
            'area_sqm': entity.get('area_sqm'),
            'url': entity.get('url'),
            'text': entity.get('text'),
            'distance': hit.distance,
            'similarity': round(similarity, 3)
        })

    return formatted_results[:n_results]

def get_collection_count():
    return collection.num_entities