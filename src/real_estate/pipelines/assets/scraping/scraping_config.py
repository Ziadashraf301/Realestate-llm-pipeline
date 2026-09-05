"""
Dynamic Configuration for Scraping Assets.
Adding any new city to CITIES automatically generates corresponding Dagster assets across all providers.
"""

# Extensible lists: any added city automatically generates dynamic Dagster pipeline assets
CITIES = ["alexandria", "cairo", "giza"]
LISTING_TYPES = ["for-sale", "for-rent"]
PROVIDERS = ["aqarmap"]  # Bayut live disabled due to WAF; Bayut data is ingested via historical dataset


def generate_scraping_config():
    """Generate all combinations of cities, listing types, and providers."""
    return [
        {"city": city, "listing_type": listing_type, "provider": provider}
        for city in CITIES
        for listing_type in LISTING_TYPES
        for provider in PROVIDERS
    ]


SCRAPING_CONFIG = generate_scraping_config()