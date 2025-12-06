import json
import os
import boto3

def scraper_report(results, logger):
    """Print detailed summary"""
    if not results:
        logger.warning("❌ No data scraped")
        return
    
    logger.info("📊 SCRAPING SUMMARY")
    logger.info(f"Total listings: {len(results)}")
    
    # Property types
    types = {}
    for listing in results:
        ptype = listing.get('property_type', 'unknown')
        types[ptype] = types.get(ptype, 0) + 1
    
    logger.info("\n📋 By Property Type:")
    for ptype, count in sorted(types.items(), key=lambda x: x[1], reverse=True):
        logger.info(f"  • {ptype}: {count}")
    
    # Price statistics
    prices = [l['price_egp'] for l in results if l.get('price_egp')]
    if prices:
        logger.info("\n💰 Price Statistics (EGP):")
        logger.info(f"  • Min: {min(prices):,.0f}")
        logger.info(f"  • Max: {max(prices):,.0f}")
        logger.info(f"  • Avg: {sum(prices)/len(prices):,.0f}")
    
    # Data completeness
    fields = ['bedrooms', 'bathrooms', 'area_sqm', 'description', 'images']
    logger.info("\n📈 Data Completeness:")
    for field in fields:
        count = sum(1 for l in results if l.get(field))
        percentage = (count / len(results)) * 100
        logger.info(f"  • {field}: {count}/{len(results)} ({percentage:.1f}%)")
    
    # Sample listings
    logger.info("📋 SAMPLE LISTINGS (first 3)")
    
    for i, listing in enumerate(results[:3], 1):
        logger.info(f"{i}. {listing['title'][:70]}")
        logger.info(f"💰 Price: {listing.get('price_text', 'N/A')}")
        logger.info(f"📍 Location: {listing['location'][:50]}")
        logger.info(f"🏠 Type: {listing['property_type']}")
        if listing.get('bedrooms'):
            logger.info(f"🛏️  {listing.get('bedrooms')} beds | 🚿 {listing.get('bathrooms')} baths | 📐 {listing.get('area_sqm')} m²")
        logger.info(f"🔗 {listing['url'][:70]}...")
        if listing.get('images'):
            logger.info(f"📸 Images: {len(listing['images'])}")
        logger.info("")


def save_to_json(filename, results, logger):
    """Save results to JSON (append mode)""" 
    # Load existing data if file exists
    existing_data = []
    if os.path.exists(filename):
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
        except json.JSONDecodeError:
            logger.warning(f"Could not read existing {filename}, starting fresh")
        
    # Merge new results with existing (avoid duplicates by property_id)
    existing_ids = {item.get('property_id') for item in existing_data}
    new_items = [item for item in results if item.get('property_id') not in existing_ids]
        
    combined_data = existing_data + new_items
        
    # Save combined data
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(combined_data, f, indent=2, ensure_ascii=False)
        
    logger.info(f"✅ Added {len(new_items)} new properties to {filename} (Total: {len(combined_data)})")


def upload_to_s3(local_file_path, bucket_name, s3_key, logger):
    """Upload a file to an S3 bucket"""
    s3 = boto3.client("s3")

    try:
        s3.upload_file(local_file_path, bucket_name, s3_key)
        logger.info(f"📤 Uploaded to S3: s3://{bucket_name}/{s3_key}")
    except Exception as e:
        logger.error(f"❌ S3 upload failed: {e}")
        raise