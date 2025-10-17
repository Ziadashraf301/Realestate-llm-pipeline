import os
import warnings
import tensorflow as tf
from pathlib import Path
import sys

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from src.real_estate_scraper import AQARMAPRealEstateScraper
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
LOG_FILE_PATH = PROJECT_ROOT / 'logs/aqarmap_scraper.log'

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

except FileNotFoundError:
    print(f"⚠️ WARNING: table_config.json not found at {TABLE_CONFIG_PATH}")
    print("   Using default fallback values...")
    GCP_PROJECT_ID = 'your-gcp-project-id'
    BQ_DATASET_ID = 'real_estate'
    BQ_TABLE_ID = 'scraped_properties'

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

print("\n" + "=" * 70)
print("🔧 Dagster Scraper Configuration")
print("=" * 70)
print(f"✅ Credentials: {GOOGLE_APPLICATION_CREDENTIALS}")
print(f"📊 GCP Project: {GCP_PROJECT_ID[:6]}****")
print(f"📊 Dataset: {BQ_DATASET_ID}")
print(f"📊 Table: {BQ_TABLE_ID}")
print("=" * 70 + "\n")

def main():
    """Main execution"""
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║   Enhanced Egyptian Real Estate Scraper v2.0             ║
    ║   Features: URL tracking, logging, skip duplicates       ║
    ║   Storage: JSON + BigQuery                               ║
    ╚══════════════════════════════════════════════════════════╝
    """)

    
    scraper = AQARMAPRealEstateScraper(
        project_id=GCP_PROJECT_ID,
        dataset_id=BQ_DATASET_ID,
        table_id=BQ_TABLE_ID,
        log_file=LOG_FILE_PATH
    )
    
    # Configuration
    CITY = 'alexandria'           # cairo, alexandria, giza, etc.
    LISTING_TYPE = 'for-sale'     # for-sale or for-rent
    MAX_PAGES = 1                # Number of pages to scrape
    
    print(f"⚙️  Configuration:")
    print(f"  • City: {CITY}")
    print(f"  • Type: {LISTING_TYPE}")
    print(f"  • Pages: {MAX_PAGES}")
    print(f"  • Deep scraping: ENABLED")
    print(f"  • URL tracking: ENABLED")
    print(f"  • Logging: ENABLED")
    print(f"  • BigQuery: {GCP_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}")
    print()
    
    try:
        # Scrape
        print("🔄 Starting scraping process...\n")
        scraper.scrape_aqarmap(
            city=CITY,
            listing_type=LISTING_TYPE,
            max_pages=MAX_PAGES
        )
        
        # Print summary
        scraper.print_summary()
        
        # Save results
        if scraper.results:
            print(f"\n{'='*60}")
            print("💾 Saving results...")
            print("="*60 + "\n")
            
            # Save to JSON
            json_file = 'aqarmap/raw_data/alexandria_for_sale.json'
            scraper.save_to_json(json_file)
            print(f"✅ Saved to JSON: {json_file}")
            
            # Save to BigQuery
            try:
                inserted_count = scraper.save_to_bigquery()
                
                if inserted_count > 0:
                    print(f"✅ Uploaded {inserted_count} properties to BigQuery")
                else:
                    print("ℹ️  No new properties uploaded to BigQuery (all were duplicates)")
                    
            except Exception as bq_error:
                print(f"⚠️  BigQuery upload failed: {bq_error}")
                print("   Data is still saved in JSON format")
            
            print(f"\n{'='*60}")
            print("✅ Scraping completed successfully!")
            print("📁 Files created:")
            print(f"   • {json_file}")
            print("   • logs/aqarmap_scraper.log (activity log)")
            if inserted_count > 0:
                print(f"   • BigQuery: {GCP_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}")
            print(f"{'='*60}\n")
        else:
            print("\n⚠️  No new properties found to save")
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Scraping interrupted by user")
        print("💾 Saving partial results...")
        if scraper.results:
            scraper.save_to_json('aqarmap/raw_data/aqarmap_scraped_properties_detailed.json')
            print("✅ Partial results saved")
        
    except Exception as e:
        print(f"\n❌ Error during scraping: {e}")
        import traceback
        traceback.print_exc()
        
        # Try to save any results collected before the error
        if scraper.results:
            print("\n💾 Attempting to save partial results...")
            try:
                scraper.save_to_json('raw_data/aqarmap_scraped_properties_detailed.json')
                print("✅ Partial results saved")
            except:
                print("❌ Failed to save partial results")


if __name__ == "__main__":
    # Check dependencies
    print("🔍 Checking dependencies...\n")
    
    missing_deps = []
    
    try:
        import requests
        print("✅ requests")
    except ImportError:
        missing_deps.append("requests")
        print("❌ requests")
    
    try:
        from bs4 import BeautifulSoup
        print("✅ beautifulsoup4")
    except ImportError:
        missing_deps.append("beautifulsoup4")
        print("❌ beautifulsoup4")
    
    try:
        from google.cloud import bigquery
        print("✅ google-cloud-bigquery")
    except ImportError:
        missing_deps.append("google-cloud-bigquery")
        print("❌ google-cloud-bigquery")
    
    if missing_deps:
        print(f"\n⚠️  Missing dependencies. Install with:")
        print(f"pip install {' '.join(missing_deps)}")
        exit(1)
    
    print("\n✅ All dependencies installed\n")
    main()