from src.logger import LoggerFactory
from schemes import get_property_schema
from pymilvus import (
    MilvusClient,
    utility,
    FieldSchema,
    CollectionSchema,
    DataType,
    Collection,
)

class Milvus_VectorDatabase():
    def __init__(self, log_dir, milvus_host, milvus_port, 
                 collection_name, embedding_dim=768):
        self.log_dir = log_dir
        self.milvus_uri = f"http://{milvus_host}:{milvus_port}"
        self.collection_name = collection_name
        self.embedding_dim = embedding_dim

        # Initialize logger
        self.logger = LoggerFactory.create_logger(log_dir=self.log_dir)
        self.client = None

    def connect(self):
        """Connect using MilvusClient"""
        self.logger.info(f"💾 Connecting to Milvus at {self.milvus_uri}...")
        try:
            self.client = MilvusClient(uri=self.milvus_uri)
            self.logger.info("✅ Successfully connected to Milvus")
        except Exception as e:
            self.logger.error(f"Milvus connection failed: {e}")
            raise ConnectionError(f"Failed to connect to Milvus") from e

    def close(self):
        """Close connection"""
        if self.client:
            self.client.close()
            self.logger.info("Disconnected from Milvus")
                    
    def create_collection(self):
        """Create collection using client API"""
        if not self.client:
            raise RuntimeError("Not connected. Call connect() first.")
        
        try:
            # Check if collection exists
            if self.client.has_collection(self.collection_name):
                self.logger.info(f"📂 Collection '{self.collection_name}' already exists")
                
                # Load collection for querying
                self.client.load_collection(self.collection_name)
                self.logger.info(f"✅ Collection '{self.collection_name}' loaded and ready")
                return
            
            self.logger.info(f"🆕 Creating new collection '{self.collection_name}'...")
            
            # Create schema using client API
            schema = self.client.create_schema(
                auto_id=False,
                enable_dynamic_field=False,
                description="Egyptian real estate properties"
            )
            
            # Add fields to schema
            schema_fields = self._get_property_schema()
            for field in schema_fields:
                schema.add_field(**field)
            
            self.logger.info(f"📋 Schema created with {len(schema_fields)} fields")
            
            # Create index for vector field
            index_params = self.client.prepare_index_params()
            index_params.add_index(
                field_name="embedding",
                index_type="IVF_FLAT",
                metric_type="COSINE",
                params={"nlist": 128}
            )
            
            self.logger.info("📊 Vector index configured (IVF_FLAT, COSINE)")
            
            # Create collection with schema and index
            self.client.create_collection(
                collection_name=self.collection_name,
                schema=schema,
                index_params=index_params
            )
            
            self.logger.info(f"✅ Collection '{self.collection_name}' created successfully!")
            self.logger.info(f" - Embedding dimension: {self.embedding_dim}")
            self.logger.info(f" - Metric type: COSINE similarity")
            self.logger.info(f" - Index type: IVF_FLAT")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to create collection: {e}")
            raise
    
        
    def get_collection_stats(self):
        """Get statistics about the vector database"""
        if not self.client:
            raise RuntimeError("Not connected. Call connect() first.")
        
        try:
            stats = self.client.get_collection_stats(self.collection_name)
            count = stats['row_count']
            
            self.logger.info("📊 MILVUS VECTOR DATABASE STATISTICS")
            self.logger.info(f"Collection name:      {self.collection_name}")
            self.logger.info(f"Total properties:     {count:,}")
            self.logger.info(f"Embedding dimension:  {self.embedding_dim}")
            self.logger.info(f"Metric type:          COSINE similarity")
            
            return count
            
        except Exception as e:
            self.logger.error(f"❌ Failed to get collection stats: {e}")
            raise

    def delete_collection(self):
        """Delete the collection (use with caution!)"""
        if not self.client:
            raise RuntimeError("Not connected. Call connect() first.")
        
        try:
            if self.client.has_collection(self.collection_name):
                self.client.drop_collection(self.collection_name)
                self.logger.info(f"🗑️ Collection '{self.collection_name}' deleted")
            else:
                self.logger.warning(f"⚠️ Collection '{self.collection_name}' does not exist")
                
        except Exception as e:
            self.logger.error(f"❌ Failed to delete collection: {e}")
            raise

    