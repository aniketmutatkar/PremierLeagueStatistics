"""
Scrapers package initialization
"""
from .base_scraper import BaseScraper
from .stats_scraper import StatsScraper
from .multi_stats_scraper import MultiStatsScraper
from .fixture_scraper import FixtureScraper

__all__ = ['BaseScraper', 'StatsScraper', 'MultiStatsScraper', 'FixtureScraper']