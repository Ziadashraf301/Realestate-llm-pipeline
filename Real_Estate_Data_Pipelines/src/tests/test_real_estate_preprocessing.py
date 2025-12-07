# ---
# 🏠 Real Estate Data Preprocessing Test
# ---
# This script validates configuration loading, BigQuery connection,
# text cleaning, preprocessing, embedding generation, and metadata creation
# from the RealEstateVectorProcessor class before full pipeline integration.
# ---

# =============================================================================
# Imports & Setup
# =============================================================================

import os
import time
import warnings
import pandas as pd
import tensorflow as tf
from pathlib import Path
from sentence_transformers import SentenceTransformer
import sys

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from Real_Estate_Data_Pipelines.src.etl.vectors_builder.real_estate_vector_processor import RealEstateMilvusProcessor
import json

# Suppress unnecessary warnings
warnings.filterwarnings("ignore")
tf.get_logger().setLevel('ERROR')

# =============================================================================
# Configuration
# =============================================================================

# Use dynamic path resolution instead of hardcoded paths
CONFIG_DIR = PROJECT_ROOT / 'config'
GOOGLE_APPLICATION_CREDENTIALS = CONFIG_DIR / 'big_query_service_account.json'
TABLE_CONFIG_PATH = CONFIG_DIR / 'table_config.json'

print("\n" + "=" * 70)
print("📂 Loading Configuration Files")
print("=" * 70)
print(f"🔍 Project Root: {PROJECT_ROOT}")
print(f"🔍 Config Directory: {CONFIG_DIR}")
print(f"🔍 Looking for: {TABLE_CONFIG_PATH}")
print(f"🔍 File exists: {TABLE_CONFIG_PATH.exists()}")
print("=" * 70 + "\n")

# Load Table Configuration
try:
    with open(TABLE_CONFIG_PATH, 'r', encoding='utf-8') as f:
        table_config = json.load(f)
    print(f"✅ Table config loaded from: {TABLE_CONFIG_PATH}")

    GCP_PROJECT_ID = table_config.get('GCP_PROJECT_ID')
    BQ_DATASET_ID = table_config.get('BQ_DATASET_ID')
    BQ_TABLE_ID = table_config.get('BQ_TABLE_ID')
    EMBEDDING_MODEL = table_config.get('EMBEDDING_MODEL')

except FileNotFoundError:
    print(f"⚠️ WARNING: table_config.json not found at {TABLE_CONFIG_PATH}")
    print("   Using default fallback values...")
    GCP_PROJECT_ID = 'your-gcp-project-id'
    BQ_DATASET_ID = 'real_estate'
    BQ_TABLE_ID = 'scraped_properties'
    EMBEDDING_MODEL = 'paraphrase-multilingual-MiniLM-L12-v2'

# Validate Configuration
if not all([GCP_PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID]):
    raise ValueError("❌ Missing required configuration values in table_config.json")

if GCP_PROJECT_ID == 'your-gcp-project-id':
    raise ValueError("❌ Please update GCP_PROJECT_ID in table_config.json with your actual project ID")

# Validate Credentials
if not GOOGLE_APPLICATION_CREDENTIALS.exists():
    raise FileNotFoundError(f"❌ Missing credentials file: {GOOGLE_APPLICATION_CREDENTIALS}")

# Set Environment Variables
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(GOOGLE_APPLICATION_CREDENTIALS)
os.environ['GCP_PROJECT_ID'] = GCP_PROJECT_ID
os.environ['BQ_DATASET_ID'] = BQ_DATASET_ID
os.environ['BQ_TABLE_ID'] = BQ_TABLE_ID
os.environ['EMBEDDING_MODEL'] = EMBEDDING_MODEL

print("\n" + "=" * 70)
print("🔧 Dagster Scraper Configuration")
print("=" * 70)
print(f"✅ Credentials: {GOOGLE_APPLICATION_CREDENTIALS}")
print(f"📊 GCP Project: {GCP_PROJECT_ID[:6]}****")
print(f"📊 Dataset: {BQ_DATASET_ID}")
print(f"📊 Table: {BQ_TABLE_ID}")
print("=" * 70 + "\n")

# =============================================================================
# Initialize Processor
# =============================================================================

try:
    processor = RealEstateMilvusProcessor(
        project_id=GCP_PROJECT_ID,
        dataset_id=BQ_DATASET_ID,
        table_id=BQ_TABLE_ID,
        embedding_model = EMBEDDING_MODEL
    )
    print("✅ RealEstateVectorProcessor initialized successfully.")
except Exception as e:
    raise RuntimeError(f"❌ Failed to initialize RealEstateVectorProcessor: {e}")

# =============================================================================
# Load Sample Data
# =============================================================================

start_time = time.time()
try:
    properties = processor.load_from_bigquery(limit=5)
    df = pd.DataFrame(properties)
    print(f"✅ Loaded {len(df)} rows from BigQuery\n")
    print(df.info())
except Exception as e:
    raise RuntimeError(f"❌ Failed to load data from BigQuery: {e}")

# =============================================================================
# Test Text Cleaning
# =============================================================================

sample_texts = [
    "شقة فاخرة بالتجمع الخامس!!✨✨ مساحة 200م 🏠",
    "Luxury Apartment - 3 Bedrooms, 2 Bathrooms, Garden View!",
    None
]

print("\n🧹 Testing text cleaning...")
for text in sample_texts:
    cleaned = processor.clean_text(text)
    print(f"\nOriginal: {text}\nCleaned: {cleaned}")
assert processor.clean_text(None) == "", "❌ clean_text() should return empty string for None"

# =============================================================================
# Test Preprocessing
# =============================================================================

print("\n🧠 Testing preprocessing on first 3 properties...")
for i, prop in enumerate(properties[:3], start=1):
    processed_text = processor.preprocess_text(prop)
    print(f"\n{'=' * 60}\n🏠 Property {i}:\n{processed_text}")

# =============================================================================
# Test Embedding Generation
# =============================================================================

print("\n⚙️ Generating sample embedding...")
model = SentenceTransformer(EMBEDDING_MODEL)

sample = processor.preprocess_text(properties[0])
t0 = time.time()
embedding = model.encode(sample)
print(f"✅ Embedding generated in {time.time() - t0:.2f}s (dim={embedding.shape[0]})")

# =============================================================================
# Summary
# =============================================================================

print("\n" + "=" * 70)
print("✅ Preprocessing pipeline test completed successfully.")
print(f"⏱️ Total execution time: {time.time() - start_time:.2f}s")
print("Next step → test vector database storage (Milvus).")
print("=" * 70)