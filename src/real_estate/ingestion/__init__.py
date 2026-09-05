"""
Ingestion package for multi-source real estate data scrapers and pipelines.
"""

from .aqarmap.scraper import run_aqarmap_scraper
from .bayut.scraper import run_bayut_scraper

__all__ = ["run_aqarmap_scraper", "run_bayut_scraper"]