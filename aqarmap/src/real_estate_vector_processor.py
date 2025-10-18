"""
Real Estate Data Preprocessing and Vector Database Storage with Milvus
Loads data from BigQuery, preprocesses text, generates embeddings, and stores in Milvus
"""
import warnings
import tensorflow as tf

# Suppress unnecessary warnings
warnings.filterwarnings("ignore")
tf.get_logger().setLevel('ERROR')

from google.cloud import bigquery
import re
from sentence_transformers import SentenceTransformer
from pymilvus import (
    connections,
    utility,
    FieldSchema,
    CollectionSchema,
    DataType,
    Collection,
)
from src.logger import real_estate_logger


class RealEstateMilvusProcessor:
    """Preprocesses real estate data and stores in Milvus vector database"""
    
    def __init__(self, project_id, dataset_id, table_id='scraped_properties',
                 milvus_host='localhost', milvus_port='19530',
                 embedding_model='all-MiniLM-L6-v2'):
        """
        Initialize processor
        
        Args:
            project_id: GCP project ID
            dataset_id: BigQuery dataset ID
            table_id: BigQuery table name
            milvus_host: Milvus server host
            milvus_port: Milvus server port
            embedding_model: Sentence transformer model name
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.table_ref = f"{project_id}.{dataset_id}.{table_id}"
        
        # Initialize logger
        logger_instance = real_estate_logger(log_file='C:/Users/MSI/OneDrive/Desktop/prtofolio/real_estate/aqarmap/logs/vector.log')
        self.logger = logger_instance.logger
        
        # Initialize BigQuery client
        self.logger.info(f"📌 Connecting to BigQuery...")
        self.bq_client = bigquery.Client(project=project_id)
        
        # Initialize embedding model
        self.logger.info(f"🤖 Loading embedding model: {embedding_model}...")
        self.model = SentenceTransformer(embedding_model)
        self.embedding_dim = self.model.get_sentence_embedding_dimension()
        
        # Connect to Milvus
        self.logger.info(f"💾 Connecting to Milvus at {milvus_host}:{milvus_port}...")
        connections.connect(
            alias="default",
            host=milvus_host,
            port=milvus_port
        )
        
        # Create or get collection
        self.collection_name = "real_estate_properties"
        self.collection = self._create_collection()
        
        self.logger.info("✅ Initialization complete!\n")
    
    def _create_collection(self):
        """Create Milvus collection with schema"""
        
        # Check if collection exists
        if utility.has_collection(self.collection_name):
            self.logger.info(f"📂 Collection '{self.collection_name}' already exists")
            collection = Collection(self.collection_name)
            collection.load()
            return collection
        
        # Define schema
        fields = [
            FieldSchema(name="property_id", dtype=DataType.VARCHAR, is_primary=True, max_length=100),
            FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=self.embedding_dim),
            FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="source", dtype=DataType.VARCHAR, max_length=200),
            FieldSchema(name="url", dtype=DataType.VARCHAR, max_length=500),
            FieldSchema(name="title", dtype=DataType.VARCHAR, max_length=500),
            FieldSchema(name="property_type", dtype=DataType.VARCHAR, max_length=100),
            FieldSchema(name="listing_type", dtype=DataType.VARCHAR, max_length=50),
            FieldSchema(name="location", dtype=DataType.VARCHAR, max_length=200),
            FieldSchema(name="price_egp", dtype=DataType.FLOAT),
            FieldSchema(name="bedrooms", dtype=DataType.INT64),
            FieldSchema(name="bathrooms", dtype=DataType.INT64),
            FieldSchema(name="area_sqm", dtype=DataType.FLOAT),
            FieldSchema(name="floor_number", dtype=DataType.INT64),
            FieldSchema(name="latitude", dtype=DataType.FLOAT),
            FieldSchema(name="longitude", dtype=DataType.FLOAT),
        ]
        
        schema = CollectionSchema(
            fields=fields,
            description="Egyptian real estate properties from AQARMAP"
        )
        
        # Create collection
        self.logger.info(f"🆕 Creating new collection '{self.collection_name}'...")
        collection = Collection(
            name=self.collection_name,
            schema=schema,
            using='default'
        )
        
        # Create index for vector field
        index_params = {
            "metric_type": "COSINE",
            "index_type": "IVF_FLAT",
            "params": {"nlist": 128}
        }
        
        collection.create_index(
            field_name="embedding",
            index_params=index_params
        )
        
        self.logger.info("✅ Collection created successfully!")
        collection.load()
        
        return collection
    
    def load_from_bigquery(self, limit=None):
        """
        Load property data from BigQuery
        
        Args:
            limit: Maximum number of rows to load (None for all)
        
        Returns:
            List of property dictionaries
        """
        query = f"""
            SELECT 
                property_id,
                source,
                url,
                title,
                description,
                price_egp,
                price_text,
                currency,
                property_type,
                listing_type,
                bedrooms,
                bathrooms,
                area_sqm,
                floor_number,
                location,
                address,
                latitude,
                longitude,
                last_updated,
                images,
                image_count,
                agent_type,
                scraped_at
            FROM `{self.table_ref}`
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        self.logger.info(f"📊 Loading data from BigQuery...")
        query_job = self.bq_client.query(query)
        results = query_job.result()
        
        properties = []
        for row in results:
            prop = dict(row.items())
            properties.append(prop)
        
        self.logger.info(f"✅ Loaded {len(properties)} properties\n")
        return properties
    
    def preprocess_text(self, prop):
        """
        Preprocess and create searchable text from property data
        
        Args:
            prop: Property dictionary
        
        Returns:
            Cleaned and formatted text string
        """
        text_parts = []
        
        # Title
        if prop.get('title'):
            text_parts.append(f"{self.clean_text(prop['title'])}")
        
        # Property type and listing type
        if prop.get('property_type'):
            text_parts.append(f"{prop['property_type']}")
        
        if prop.get('listing_type'):
            text_parts.append(f"{prop['listing_type']}")
        
        # Price
        if prop.get('price_egp'):
            text_parts.append(f"{prop['price_egp']:,.0f} جنيه")
        
        # Location
        if prop.get('location'):
            text_parts.append(f"{prop['location']}")
        
        if prop.get('address'):
            text_parts.append(f"{self.clean_text(prop['address'])}")
        
        # Property details
        details = []
        if prop.get('bedrooms'):
            details.append(f"{prop['bedrooms']} غرف")
        if prop.get('bathrooms'):
            details.append(f"{prop['bathrooms']} حمام")
        if prop.get('area_sqm'):
            details.append(f"{prop['area_sqm']} متر مربع")
        if prop.get('floor_number'):
            details.append(f"{prop['floor_number']} ادوار")
        
        if details:
            text_parts.append(f"{' '.join(details)}")
        
        # Description
        if prop.get('description'):
            text_parts.append(f"{self.clean_text(prop['description'])}")
        
        return " ".join(text_parts)
    
    def clean_text(self, text):
        """
        Clean Arabic real estate text:
        - Replace bullets with readable dashes
        - Normalize Arabic letters
        - Remove Arabic commas, slashes, emojis, and extra symbols
        - Keep Arabic, English, numbers, and key punctuation
        """
        if not text:
            return ""

        text = str(text)

        # Replace bullets and unusual dots with spaced hyphens
        text = re.sub(r'[▪•●◼◾▫◽]', ' ', text)

        # Replace newlines/tabs with a single space
        text = re.sub(r'[\n\r\t]+', ' ', text)

        # Normalize Arabic letters
        text = re.sub(r'[إأآا]', 'ا', text)
        text = re.sub(r'[يى]', 'ي', text)
        text = re.sub(r'[ؤئ]', 'ء', text)

        # Remove Arabic commas, slashes, emojis, and other unwanted symbols
        text = re.sub(r'[،/!؟💰:()+%.,-]', ' ', text)

        # Keep only allowed characters: Arabic, English, digits, and basic punctuation
        text = re.sub(r'[^\w\s\u0600-\u06FF]', '', text)

        # Collapse multiple spaces
        text = re.sub(r'\s+', ' ', text)

        return text.strip()
    
    def process_and_store(self, properties, batch_size=100):
        """
        Process properties and store in Milvus vector database
        
        Args:
            properties: List of property dictionaries
            batch_size: Number of properties to process at once
        
        Returns:
            Number of properties stored
        """
        self.logger.info(f"🔄 Processing {len(properties)} properties...")
        
        stored_count = 0
        
        for i in range(0, len(properties), batch_size):
            batch = properties[i:i + batch_size]
            
            # Prepare batch data
            batch_data = {
                "property_id": [],
                "embedding": [],
                "text": [],
                "source": [],
                "url": [],
                "title": [],
                "property_type": [],
                "listing_type": [],
                "location": [],
                "price_egp": [],
                "bedrooms": [],
                "bathrooms": [],
                "area_sqm": [],
                "floor_number": [],
                "latitude": [],
                "longitude": []
            }
            
            for prop in batch:
                # Create unique ID
                prop_id = prop.get('property_id')
                if not prop_id:
                    continue
                
                # Preprocess text
                text = self.preprocess_text(prop)
                if not text:
                    continue
                
                # Generate embedding
                embedding = self.model.encode(text, convert_to_numpy=True, normalize_embeddings=True,)
                
                # Add to batch
                batch_data["property_id"].append(str(prop_id))
                batch_data["embedding"].append(embedding.tolist())
                batch_data["text"].append(text)
                batch_data["source"].append(str(prop.get('source', '')))
                batch_data["url"].append(str(prop.get('url', '')))
                batch_data["title"].append(str(prop.get('title', '')))
                batch_data["property_type"].append(str(prop.get('property_type', '')))
                batch_data["listing_type"].append(str(prop.get('listing_type', '')))
                batch_data["location"].append(str(prop.get('location', '')))
                batch_data["price_egp"].append(float(prop.get('price_egp', 0)) if prop.get('price_egp') else 0.0)
                batch_data["bedrooms"].append(int(prop.get('bedrooms', 0)) if prop.get('bedrooms') else 0)
                batch_data["bathrooms"].append(int(prop.get('bathrooms', 0)) if prop.get('bathrooms') else 0)
                batch_data["area_sqm"].append(float(prop.get('area_sqm', 0)) if prop.get('area_sqm') else 0.0)
                batch_data["floor_number"].append(int(prop.get('floor_number', 0)) if prop.get('floor_number') else 0)
                batch_data["latitude"].append(float(prop.get('latitude', 0)) if prop.get('latitude') else 31)
                batch_data["longitude"].append(float(prop.get('longitude', 0)) if prop.get('longitude') else 29)
            
            # Insert batch into Milvus
            if batch_data["property_id"]:
                try:
                    self.collection.insert(list(batch_data.values()))
                    self.collection.flush()
                    stored_count += len(batch_data["property_id"])
                    self.logger.info(f"  ✅ Processed batch {i//batch_size + 1}: {len(batch_data['property_id'])} properties")
                except Exception as e:
                    self.logger.error(f"  ❌ Error processing batch {i//batch_size + 1}: {e}")
        
        self.logger.info(f"\n✅ Stored {stored_count} properties in Milvus vector database\n")
        return stored_count
    
    def search_properties(self, query, n_results=5, filter_expr=None):
        """
        Search for properties using natural language query
        
        Args:
            query: Search query string
            n_results: Number of results to return
            filter_expr: Optional filter expression (e.g., 'location == "alexandria"')
        
        Returns:
            Search results
        """
        self.logger.info(f"🔍 Searching for: '{query}'")
        
        # Generate query embedding
        query_embedding = self.model.encode(query, convert_to_numpy=True)
        
        # Define search parameters
        search_params = {
            "metric_type": "L2",
            "params": {"nprobe": 10}
        }
        
        # Search in Milvus
        results = self.collection.search(
            data=[query_embedding.tolist()],
            anns_field="embedding",
            param=search_params,
            limit=n_results,
            expr=filter_expr,
            output_fields=["property_id", "title", "location", "property_type", 
                          "listing_type", "price_egp", "bedrooms", "bathrooms", 
                          "area_sqm", "url", "text"]
        )
        
        return results
    
    def print_search_results(self, results):
        """Print formatted search results"""
        if not results or len(results[0]) == 0:
            self.logger.info("❌ No results found\n")
            return
        
        self.logger.info(f"\n{'='*80}")
        self.logger.info(f"🏠 Found {len(results[0])} Results")
        self.logger.info("="*80 + "\n")
        
        for i, hit in enumerate(results[0], 1):
            entity = hit.entity
            self.logger.info(f"{i}. {entity.get('title', 'N/A')[:70]}")
            self.logger.info(f"   💰 Price: {entity.get('price_egp', 0):,.0f} EGP")
            self.logger.info(f"   📍 Location: {entity.get('location', 'N/A')}")
            self.logger.info(f"   🏠 Type: {entity.get('property_type', 'N/A')}")
            
            if entity.get('bedrooms'):
                self.logger.info(f"   🛏️  {entity.get('bedrooms')} beds | 🚿 {entity.get('bathrooms')} baths | 📐 {entity.get('area_sqm')} m²")
            
            self.logger.info(f"   🔗 {entity.get('url', 'N/A')[:70]}...")
            self.logger.info(f"   📊 Distance: {hit.distance:.3f}\n")
    
    def get_collection_stats(self):
        """Get statistics about the vector database"""
        count = self.collection.num_entities
        
        self.logger.info(f"\n{'='*60}")
        self.logger.info("📊 Milvus Vector Database Statistics")
        self.logger.info("="*60)
        self.logger.info(f"Total properties: {count}")
        self.logger.info(f"Collection name: {self.collection_name}")
        self.logger.info(f"Embedding dimension: {self.embedding_dim}")
        self.logger.info("="*60 + "\n")
        
        return count