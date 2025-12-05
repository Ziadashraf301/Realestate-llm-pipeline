import os
from pathlib import Path
from src.scrapers.aqarmap_real_estate_scraper import AQARMAPRealEstateScraper
import warnings
from src.config import Config
from src.databases import Big_Query_Database

def main():
    """Main execution"""
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║   Enhanced Egyptian Real Estate Scraper v2.0             ║
    ║   Features: URL tracking, logging, skip duplicates       ║
    ║   Storage: JSON + BigQuery                               ║
    ╚══════════════════════════════════════════════════════════╝
    """)

    # Suppress unnecessary warnings
    warnings.filterwarnings("ignore")

    PROJECT_ROOT = Path(__file__).resolve().parents[3]
    CONFIG_DIR = PROJECT_ROOT / "Configs"
    CONFIG_PATH = CONFIG_DIR / "Real_Estate_Data_Pipelines.json"
    print(CONFIG_PATH)
    GOOGLE_APPLICATION_CREDENTIALS = CONFIG_DIR / "big_query_service_account.json"

    # Load Config
    cfg = Config(CONFIG_PATH)

    # Set env variables dynamically
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(GOOGLE_APPLICATION_CREDENTIALS)
    os.environ["GCP_PROJECT_ID"] = cfg.GCP_PROJECT_ID
    os.environ["BQ_DATASET_ID"] = cfg.BQ_RAW_DATASET_ID
    os.environ["BQ_TABLE_ID"] = cfg.BQ_RAW_TABLE_ID

    # Log
    print("✅ Configuration loaded dynamically from:", CONFIG_PATH)
    print(f"GCP_PROJECT_ID: {cfg.GCP_PROJECT_ID}")
    print(f"BQ_DATASET_ID: {cfg.BQ_RAW_DATASET_ID}")
    print(f"BQ_TABLE_ID: {cfg.BQ_RAW_TABLE_ID}")

    bg_database = Big_Query_Database(
        project_id=os.environ["GCP_PROJECT_ID"],
        dataset_id=os.environ["BQ_DATASET_ID"],
        table_id=os.environ["BQ_TABLE_ID"],
        log_dir=cfg.LOG_DIR, 
    )

    scraper = AQARMAPRealEstateScraper(
        log_dir=cfg.LOG_DIR,
        db_client=bg_database
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
            json_file = 'C:/Users/MSI/OneDrive/Desktop/real_estate/Real_Estate_Data_Pipelines/raw_data/alexandria_for_sale.json'
            scraper.save_to_json(json_file)
            print(f"✅ Saved to JSON: {json_file}")
            
            # Save to BigQuery
            try:
                inserted_count = bg_database.save_to_bigquery(scraper.results)
                
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
            print(f"{'='*60}\n")
        else:
            print("\n⚠️  No new properties found to save")
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Scraping interrupted by user")
        print("💾 Saving partial results...")
        if scraper.results:
            scraper.save_to_json('C:/Users/MSI/OneDrive/Desktop/real_estate/Real_Estate_Data_Pipelines/raw_data/aqarmap_scraped_properties_detailed.json')
            print("✅ Partial results saved")
        
    except Exception as e:
        print(f"\n❌ Error during scraping: {e}")
        import traceback
        traceback.print_exc()
        
        # Try to save any results collected before the error
        if scraper.results:
            print("\n💾 Attempting to save partial results...")
            try:
                scraper.save_to_json('C:/Users/MSI/OneDrive/Desktop/real_estate/Real_Estate_Data_Pipelines/raw_data/aqarmap_scraped_properties_detailed.json')
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