from src.logger import LoggerFactory
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import tempfile
import json as json_lib
from datetime import datetime
from .schemes import PropertySchema
from ..db_models import PropertyModel
import os

class Big_Query_Database():
    def __init__(self,
                project_id, 
                dataset_id, 
                table_id,
                log_dir):
        
        # BigQuery configuration
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.table_ref = f"{project_id}.{dataset_id}.{table_id}"
        self.log_dir = log_dir
        
        # Initialize logger
        self.logger = LoggerFactory.create_logger(log_dir=self.log_dir)

        # Initialize BigQuery client
        try:
            self.bq_client = bigquery.Client(project=project_id)
            self.logger.info(f"✅ Connected to BigQuery project: {project_id}")
        except Exception as e:
            self.logger.info(f"❌ Failed to connect to BigQuery: {e}")
            raise


    def _load_existing_urls_from_database(self):
        """Load existing property URLs from BigQuery"""
        try:
            query = f"""
                SELECT DISTINCT url 
                FROM `{self.table_ref}`
            """
            self.logger.info("🔍 Loading existing URLs from BigQuery...")
            query_job = self.bq_client.query(query)
            existing_urls = {row.url for row in query_job.result()}
            self.logger.info(f"📂 Loaded {len(existing_urls)} existing URLs from BigQuery")
            return existing_urls
        
        except Exception as e:
            self.logger.info(f"⚠️  Could not load existing URLs (table may not exist yet): {e}")
            return set()
        
    def check_table_exists(self):
        try:
            _ = self.bq_client.get_table(self.table_ref)
            table_exists = True
            self.logger.info(f"✅ Table {self.table_ref} exists")
        except NotFound:
            table_exists = False
            self.logger.info(f"⚠️  Table {self.table_ref} does not exist, will create it")
        return table_exists
    

    def create_table_if_not_exists(self):

        table_exists = self.check_table_exists()

        # Create table if not exists
        if not table_exists:
            try:
                table = bigquery.Table(self.table_ref, schema=PropertySchema)
                table = self.bq_client.create_table(table)
                self.logger.info(f"✅ Created table {self.table_ref}")
                return 1
            except Exception as e:
                self.logger.error(f"❌ Failed to create table: {e}")
                return 0

    def save_to_database(self, results):
        """Save results to BigQuery using batch load (free tier compatible)"""
        if not results:
            self.logger.warning("No data to save")
            return 0
        
        _ = self.create_table_if_not_exists()

        self.logger.info("📤 Uploading to BigQuery (Batch Mode)")

        # Prepare data for BigQuery
        new_items = []
        current_time = datetime.utcnow()
        
        for item in results:
            # Prepare item for BigQuery
            bq_item = {
                'property_id': item.get('property_id'),
                'source': item.get('source'),
                'url': item.get('url'),
                'title': item.get('title'),
                'description': item.get('description'),
                'price_egp': item.get('price_egp'),
                'price_text': item.get('price_text'),
                'currency': item.get('currency'),
                'property_type': item.get('property_type'),
                'listing_type': item.get('listing_type'),
                
                # Property details
                'bedrooms': item.get('bedrooms'),
                'bathrooms': item.get('bathrooms'),
                'area_sqm': item.get('area_sqm'),
                'floor_number': item.get('floor_number'),
                
                # Location details
                'location': item.get('location'),
                'address': item.get('address'),
                'latitude': item.get('latitude'),
                'longitude': item.get('longitude'),
                'last_updated': item.get('last_updated'),
                
                # Images (convert list to JSON string)
                'images': json_lib.dumps(item.get('images', [])),
                'image_count': len(item.get('images', [])),
                
                # Agent information
                'agent_type': item.get('agent_type'),
                
                # Timestamps
                'scraped_at': item.get('scraped_at'),
                'loaded_at': current_time.isoformat(),
            }
            
            try:
                validated_item = PropertyModel(**bq_item)
                new_items.append(validated_item.dict())
            except Exception as e:
                self.logger.error(f"❌ Invalid row skipped: {e}")
        
        # Use batch load with temporary JSON file (FREE TIER COMPATIBLE)
        self.logger.info(f"📤 Batch loading {len(new_items)} new properties...")
        try:
            # Create temporary NDJSON file (newline-delimited JSON)
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as temp_file:
                for item in new_items:
                    json_lib.dump(item, temp_file)
                    temp_file.write('\n')
                temp_file_path = temp_file.name
            
            # Configure load job
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                schema=PropertySchema,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                autodetect=False
            )
            
            # Load data from file
            with open(temp_file_path, 'rb') as source_file:
                load_job = self.bq_client.load_table_from_file(
                    source_file,
                    self.table_ref,
                    job_config=job_config
                )
            
            # Wait for job to complete
            self.logger.info("⏳ Waiting for load job to complete...")
            load_job.result()
            
            # Clean up temp file
            os.unlink(temp_file_path)
            
            self.logger.info("✅ BigQuery Upload Summary:")
            self.logger.info(f"🆕 New properties inserted: {len(new_items)}")
            self.logger.info(f"🗂️ Table: {self.table_ref}")
            return len(new_items)
            
        except Exception as e:
            self.logger.error(f"❌ Failed to load data: {e}")
            # Try to clean up temp file
            try:
                if 'temp_file_path' in locals():
                    os.unlink(temp_file_path)
            except:
                pass
            return 0
