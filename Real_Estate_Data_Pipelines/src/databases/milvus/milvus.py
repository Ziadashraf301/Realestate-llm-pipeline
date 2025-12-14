from pymilvus import MilvusClient
from typing import List, Dict, Any
from src.logger import LoggerFactory
from .schemes import get_property_schema
from ..db_models import PropertyVectorsModel

class Milvus_VectorDatabase():

    def __init__(self, log_dir, milvus_host, milvus_port, 
                 collection_name, embedding_dim=768):
        self.log_dir = log_dir
        self.milvus_uri = f"http://{milvus_host}:{milvus_port}"
        self.embedding_dim = embedding_dim
        self.collection_name = f"{collection_name}_{self.embedding_dim}"

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
                self.logger.info(f"📂 Collection {self.collection_name} already exists")
                
                # Load collection for querying
                self.client.load_collection(self.collection_name)
                self.logger.info(f"✅ Collection {self.collection_name} loaded and ready")
                return
            
            self.logger.info(f"🆕 Creating new collection {self.collection_name}...")
            
            # Create schema using client API
            schema = self.client.create_schema(
                auto_id=False,
                enable_dynamic_field=False,
                description="Egyptian real estate properties"
            )
            
            # Add fields to schema
            schema_fields = get_property_schema(self.embedding_dim)
            for field in schema_fields["fields"]:
                schema.add_field(**field)
            
            self.logger.info(f"📋 Schema created with {len(schema_fields['fields'])} fields")
            
            # Create index for vector field
            index_params = self.client.prepare_index_params()
            
            # Add vector index for embeddings
            index_params.add_index(
                field_name="embedding",
                index_type="IVF_FLAT",
                metric_type="COSINE",
                params={"nlist": 128}
            )
            
            # Add scalar index for property_id
            index_params.add_index(
                field_name="property_id",
                index_type="Trie",
                metric_type="L2"
            )
            
            self.logger.info("📊 Indexes configured:")
            self.logger.info("  - Vector: embedding (IVF_FLAT, COSINE)")
            self.logger.info("  - Scalar: property_id (TRIE)")
            
            # Create collection with schema and index
            self.client.create_collection(
                collection_name=self.collection_name,
                schema=schema,
                index_params=index_params
            )
            
            self.logger.info(f"✅ Collection '{self.collection_name}' created successfully!")
            self.logger.info(f" - Embedding dimension: {self.embedding_dim}")
            self.logger.info(f" - Metric type: COSINE similarity")
            self.logger.info(f" - Vector index: IVF_FLAT")
            self.logger.info(f" - Scalar index: property_id (TRIE)")
            
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

    
    def insert_properties(self, properties: List[Dict[str, Any]], 
                         batch_size: int = 1000) -> Dict[str, Any]:
        """
        Insert properties with validation and batching.
        
        Returns:
            Dict with statistics: {'total', 'inserted', 'failed', 'failed_records'}
        """
        if not self.client:
            raise RuntimeError("Not connected. Call connect() first.")
        
        if not properties:
            self.logger.warning("No properties to insert")
            return {'total': 0, 'inserted': 0, 'failed': 0, 'failed_records': []}
        
        
        total_inserted = 0
        failed_records = []
        total_batches = (len(properties) + batch_size - 1) // batch_size
        
        self.logger.info(f"📥 Inserting {len(properties):,} properties...")
        
        for i in range(0, len(properties), batch_size):
            batch = properties[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            batch_validated = []
            
            # Validate batch
            for prop in batch:
                try:
                    validated = PropertyVectorsModel(**prop)
                    batch_validated.append(validated.model_dump())
                except Exception as e:
                    self.logger.warning(f"Validation failed for {prop.get('property_id')}: {e}")
                    failed_records.append({
                        'property_id': prop.get('property_id'),
                        'error': str(e)
                    })
            
            # Insert validated batch
            if batch_validated:
                try:
                    result = self.client.upsert(
                        collection_name=self.collection_name,
                        data=batch_validated
                    )
                    inserted = result.get('insert_count', len(batch_validated))
                    total_inserted += inserted
                    
                    if batch_num % 10 == 0 or batch_num == total_batches:
                        self.logger.info(
                            f"   Batch {batch_num}/{total_batches}: "
                            f"{total_inserted:,} inserted"
                        )
                except Exception as e:
                    self.logger.error(f"❌ Batch {batch_num} insert failed: {e}")
                    for prop in batch_validated:
                        failed_records.append({
                            'property_id': prop.get('property_id'),
                            'error': f"Insert failed: {str(e)}"
                        })
        
        success_rate = (total_inserted / len(properties) * 100) if properties else 0
        
        self.logger.info(f"✅ Insert complete: {total_inserted:,}/{len(properties):,} "
                        f"({success_rate:.1f}% success)")
        
        if failed_records:
            self.logger.warning(f"⚠️ {len(failed_records)} records failed")
        
        return {
            'total': len(properties),
            'inserted': total_inserted,
            'failed': len(failed_records),
            'failed_records': failed_records
        }
    

    def get_property_ids(self, batch_size: int = 10_000) -> List[Any]:
        """
        Fetch all unique property_ids from the Milvus collection using cursor-based pagination.
        """
        if not self.client:
            raise RuntimeError("Not connected. Call connect() first.")
        
        # Ensure collection is loaded and property_id has index
        self.client.load_collection(self.collection_name)
        
        self.logger.info("🔍 Fetching unique property_ids from Milvus...")
        
        unique_ids = set()
        last_property_id = None
        total_fetched = 0
        iteration = 0
        
        try:
            while True:
                iteration += 1
                
                # Build filter for cursor-based pagination
                filter_expression = ""
                if last_property_id is not None:
                    filter_expression = f'property_id > "{last_property_id}"'
                
                # Query with index support
                results = self.client.query(
                    collection_name=self.collection_name,
                    filter=filter_expression,
                    output_fields=["property_id"],
                    limit=batch_size,
                    order_by=[("property_id", "ASC")]
                )
                
                if not results:
                    break
                
                batch_count = len(results)
                total_fetched += batch_count
                
                for row in results:
                    if "property_id" in row:
                        unique_ids.add(row["property_id"])
                        last_property_id = row["property_id"]
                
                # Log progress every 10 iterations
                if iteration % 10 == 0:
                    self.logger.debug(
                        f"Progress: iteration {iteration}, "
                        f"fetched {total_fetched:,} records"
                    )
                
                # Exit when we've fetched all records
                if batch_count < batch_size:
                    break
            
            self.logger.info(
                f"✅ Retrieved {len(unique_ids):,} unique property_ids "
                f"from {total_fetched:,} total records"
            )
            
            return sorted(list(unique_ids))
            
        except Exception as e:
            self.logger.error(f"❌ Failed to fetch property_ids: {e}")      
            raise