"""
Ingestion Module (Web Scraping & Data Extraction).
Contains scrapers for Egyptian real estate platforms (AQARMAP, Bayut).
"""

from .aqarmap import AsyncAQARMAPScraper
from .bayut import AsyncBayutScraper

__all__ = [
    "AsyncAQARMAPScraper",
    "AsyncBayutScraper",
]