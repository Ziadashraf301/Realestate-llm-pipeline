"""
Real Estate Data Preprocessing and Vector Database Storage with Milvus
Loads data from BigQuery, preprocesses text, generates embeddings, and stores in Milvus
"""
import warnings
from typing import List, Dict, Any, Optional

# Suppress unnecessary warnings
warnings.filterwarnings("ignore")

import json
from pathlib import Path
from Real_Estate_Data_Pipelines.src.logger import LoggerFactory


class PropertyVectorBuilder:
    """Preprocesses real estate data and stores in Milvus vector database"""
    
    def __init__(self, log_dir, 
                 rdbms_client,  
                 vectordb_client, 
                 text_preprocessor,
                 embedding_service):
        """
        Initialize processor
        
        Args:
            embedding_model: Sentence transformer model name
        """
        
        # Initialize logger
        self.log_dir = log_dir
        self.logger = LoggerFactory.create_logger(log_dir=self.log_dir)
        self.preprocessor = text_preprocessor
        self.embedder = embedding_service

        # Check the RDBMS connection
        if rdbms_client.client is None:
            self.logger.error("Relational Database connection failed")
            raise ConnectionError("Relational Database connection failed")
        else:
            self.rdbms_client = rdbms_client

        # Check the Vectordb connection
        if vectordb_client.client is None:
            self.logger.error("Vector Database connection failed")
            raise ConnectionError("Vector Database connection failed")
        else:
            self.vectordb_client = vectordb_client

    
    def transform_properties(self, properties: List[Dict], embed_batch_size: int = 64,) -> List[Dict]:
        """
        Transform properties in batches:
        - create searchable text
        - generate embeddings in batches
        """
        from tqdm import tqdm
        
        transformed = []

        texts = []
        valid_props = []

        # Prepare texts
        for prop in tqdm(properties, desc="Preparing text"):
            try:
                text = self.preprocessor.create_searchable_text(prop)

                if not text or len(text) < 10:
                    self.logger.warning(
                        f"Property {prop.get('property_id')} has insufficient text"
                    )
                    continue

                texts.append(text)
                valid_props.append(prop)

            except Exception as e:
                self.logger.error(
                    f"Text preprocessing failed for {prop.get('property_id')}: {e}"
                )

        if not texts:
            return []
        
        # Embed in batches
        self.logger.info(f"Generating embeddings for {len(texts):,} properties...")

        embeddings = self.embedder.encode_batch(
            texts,
            is_query = False,
            batch_size=embed_batch_size,
            normalize=True
        )

        # Attach embeddings back
        for prop, text, embedding in zip(valid_props, texts, embeddings):
            prop["text"] = text
            prop["embedding"] = embedding.tolist()
            transformed.append(prop)

        self.logger.info(f"✅ Transformed {len(transformed):,} properties")
        return transformed

    def process_store_to_vdb(self, limit: Optional[int] = None, batch_size: int = 1000) -> Dict[str, Any]:
        """
        Run the pipeline.
        
        Args:
            limit: Max properties to process
            batch_size: Batch size for Milvus insert
            
        Returns:
            Statistics dict
        """
        self.logger.info("Starting Vector ETL Pipeline...")

        # Extract the ids properties from the vectordb
        vectodb_ids = self.vectordb_client.get_property_ids()

        # Extract from RDBMS
        self.logger.info("Extracting data from DATABASE...")
        properties = self.rdbms_client.get_validated_properties_for_vectordb(limit=limit, 
                                                                             exclude_ids = vectodb_ids)
        
        if not properties:
            self.logger.warning("No properties to process")
            return {'total': 0, 'inserted': 0, 'failed': 0}
        
        # Transform (preprocess + embed)
        self.logger.info(f"Transforming {len(properties):,} properties...")
        transformed_properties = self.transform_properties(properties, batch_size)
        
        # Load into VECTORDB
        self.logger.info(f"Loading into VECTORDB...")
        results = self.vectordb_client.insert_properties(
            transformed_properties, 
            batch_size=batch_size
        )
        
        # Save failed records
        if results['failed_records']:
            self.save_failed_records(results['failed_records'])
        
        # Summary
        self.print_summary(results)
        
        return results


    def save_failed_records(self, failed_records: List[Dict]):
        """Save failed records to JSON file"""
        log_path = Path(self.logger.handlers[0].baseFilename).parent
        output_file = log_path / 'validation_failures.json'
        
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(failed_records, f, indent=2, ensure_ascii=False)
            
            self.logger.warning(
                f"⚠️ Saved {len(failed_records)} failed records to {output_file}"
            )
        except Exception as e:
            self.logger.error(f"Failed to save error log: {e}")
    
    def print_summary(self, results: Dict):
        """Print pipeline summary"""
        self.logger.info("📊 PIPELINE SUMMARY")
        self.logger.info(f"Total properties:     {results['total']:,}")
        self.logger.info(f"Successfully inserted: {results['inserted']:,}")
        self.logger.info(f"Failed:               {results['failed']:,}")
        
        if results['total'] > 0:
            success_rate = (results['inserted'] / results['total']) * 100
            self.logger.info(f"Success rate:         {success_rate:.1f}%")