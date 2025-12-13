"""
Configuration for scraping assets
"""

# Generate from lists
CITIES = ["alexandria", "cairo"]
LISTING_TYPES = ["for-sale", "for-rent"]

def generate_scraping_config():
    """Generate all combinations of cities and listing types"""
    return [
        {"city": city, "listing_type": listing_type}
        for city in CITIES
        for listing_type in LISTING_TYPES
    ]

SCRAPING_CONFIG = generate_scraping_config()