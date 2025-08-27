"""
Base Scraper - Uses your proven webscraper.py logic
Handles both test files and live URLs
"""
import requests
from bs4 import BeautifulSoup, Comment
import pandas as pd
import logging
import random
from io import StringIO
from pathlib import Path
from typing import List, Union
import yaml
import time

logger = logging.getLogger(__name__)

class BaseScraper:
    """
    Base scraper using your exact working logic from archive_v1/src/webscraper.py
    """
    
    def __init__(self, config_path: str = "config/scraping.yaml"):
        self.config = self._load_config(config_path)
        logger.info("Base scraper initialized")
    
    def _load_config(self, config_path: str) -> dict:
        """Load scraping configuration"""
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    
    def scrape_tables(self, source: Union[str, Path]) -> List[pd.DataFrame]:
        """
        Extract tables using your exact scrapeData() logic
        
        Args:
            source: File path or URL
            
        Returns:
            List of pandas DataFrames
        """
        logger.info(f"Starting scrape of {source}")
        
        # Get HTML content
        if isinstance(source, (str, Path)) and Path(source).exists():
            # Read from file (testing)
            logger.info(f"Reading HTML from file: {source}")
            with open(source, 'r', encoding='utf-8') as file:
                html_content = file.read()
        
        elif isinstance(source, str) and source.startswith('http'):
            # Fetch from URL (production) 
            logger.info(f"Fetching HTML from URL: {source}")
            html_content = self._fetch_url(source)
        
        else:
            raise ValueError(f"Invalid source: {source}")
        
        # Extract tables using your exact logic
        tables = self._extract_tables_from_html(html_content)
        
        logger.info(f"Successfully extracted {len(tables)} tables")
        return tables
    
    def _fetch_url(self, url: str) -> str:
        """Fetch URL using your exact logic from webscraper.py with rate limiting"""
        # Apply rate limiting delay before request
        delay = self.config['scraping']['delays']['between_requests']
        logger.info(f"Applying {delay}s delay before request...")
        time.sleep(delay)
        
        max_attempts = self.config['scraping']['retries']['max_attempts']
        backoff_factor = self.config['scraping']['retries']['backoff_factor']
        
        for attempt in range(max_attempts):
            try:
                # Your user agent rotation logic
                user_agents = self.config['scraping']['http']['user_agents']
                user_agent = random.choice(user_agents)
                
                headers = self.config['scraping']['http']['headers'].copy()
                headers['User-Agent'] = user_agent
                
                # Make request
                timeout = self.config['scraping']['http']['timeout']
                logger.info(f"Fetching {url} (attempt {attempt + 1}/{max_attempts})")
                response = requests.get(url, headers=headers, timeout=timeout)
                response.raise_for_status()
                
                logger.info(f"Successfully fetched {url}")
                return response.text
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request attempt {attempt + 1} failed: {e}")
                
                if attempt < max_attempts - 1:  # Not the last attempt
                    error_delay = self.config['scraping']['delays']['on_error']
                    wait_time = error_delay * (backoff_factor ** attempt)
                    logger.info(f"Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"All {max_attempts} attempts failed for {url}")
                    raise e  # Re-raise the last exception
        
        # This line should never be reached, but satisfies type checker
        raise RuntimeError(f"Unexpected error: failed to fetch {url}")

    def _extract_tables_from_html(self, html_content: str) -> List[pd.DataFrame]:
        """
        Extract tables using your EXACT logic from webscraper.py
        This is the critical part that was working
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        dfs = []
        
        try:
            # Find all table tags (your exact logic)
            tables = soup.find_all('table')
            for table in tables:
                df = pd.read_html(StringIO(str(table)))[0]
                dfs.append(df)
                logger.debug("Extracted table from main content")
            
            # Find all tables within the page's comments (YOUR CRITICAL DISCOVERY!)
            comments = soup.find_all(string=lambda text: isinstance(text, Comment))
            
            for comment in comments:
                comment_content = str(comment)
                
                # Check if comment content has any HTML tags before parsing
                if '<' in comment_content and '>' in comment_content:
                    comment_soup = BeautifulSoup(comment_content, 'html.parser')
                    # Find table containers in comments
                    table_container = comment_soup.find('div', class_='table_container')
                    
                    if table_container:
                        df = pd.read_html(StringIO(str(table_container)))[0]
                        dfs.append(df)
                        logger.debug("Extracted table from comment content")
            
            logger.info(f"Total tables extracted: {len(dfs)}")
            return dfs
            
        except Exception as e:
            logger.error(f"Error extracting tables: {e}")
            return []