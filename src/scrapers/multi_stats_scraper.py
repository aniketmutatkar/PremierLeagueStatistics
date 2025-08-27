"""
Multi-Stats Scraper - Config-driven system for all stat categories
Reads config/sources.yaml to determine what stats to scrape
"""
import yaml
import pandas as pd
import logging
from pathlib import Path
from typing import Dict, List, Optional
from .stats_scraper import StatsScraper

logger = logging.getLogger(__name__)

class MultiStatsScraper:
    """
    Scraper that handles multiple stat categories based on configuration
    """
    
    def __init__(self, sources_config_path: str = "config/sources.yaml"):
        self.config = self._load_sources_config(sources_config_path)
        self.base_scraper = StatsScraper()
        logger.info(f"Multi-stats scraper initialized with {len(self.config['stats_sources'])} stat categories")
    
    def _load_sources_config(self, config_path: str) -> dict:
        """Load sources configuration"""
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    
    def get_available_stat_types(self) -> List[str]:
        """Get list of all available stat types from config"""
        return list(self.config['stats_sources'].keys())
    
    def scrape_stat_category(self, stat_type: str, source_path: str) -> Dict[str, pd.DataFrame]:
        """
        Scrape a single stat category
        
        Args:
            stat_type: e.g., 'standard', 'passing', 'shooting'
            source_path: Path to HTML file or URL
            
        Returns:
            Dict with squad_{stat_type}, opponent_{stat_type}, player_{stat_type}
        """
        if stat_type not in self.config['stats_sources']:
            raise ValueError(f"Unknown stat type: {stat_type}. Available: {self.get_available_stat_types()}")
        
        logger.info(f"Scraping {stat_type} stats from {source_path}")
        
        # Use our existing StatsScraper with the stat_type parameter
        result = self.base_scraper.scrape_stats(source_path, stat_type)
        
        if result:
            logger.info(f"✅ Successfully scraped {stat_type}: {len(result)} tables")
            for table_name, df in result.items():
                logger.info(f"  {table_name}: {df.shape[0]} rows, {df.shape[1]} columns")
        else:
            logger.error(f"❌ Failed to scrape {stat_type}")
        
        return result
    
    def scrape_multiple_categories(self, stat_sources: Dict[str, str]) -> Dict[str, pd.DataFrame]:
        """
        Scrape multiple stat categories
        
        Args:
            stat_sources: Dict mapping stat_type -> source_path
            e.g., {'standard': 'data/standard.html', 'passing': 'data/passing.html'}
            
        Returns:
            Dict with all tables from all stat categories
        """
        all_tables = {}
        successful_stats = []
        failed_stats = []
        
        for stat_type, source_path in stat_sources.items():
            try:
                stat_tables = self.scrape_stat_category(stat_type, source_path)
                all_tables.update(stat_tables)
                successful_stats.append(stat_type)
                
            except Exception as e:
                logger.error(f"Failed to scrape {stat_type}: {e}")
                failed_stats.append(stat_type)
        
        logger.info(f"Scraping complete: {len(successful_stats)} successful, {len(failed_stats)} failed")
        logger.info(f"✅ Successful: {successful_stats}")
        if failed_stats:
            logger.warning(f"❌ Failed: {failed_stats}")
        
        return all_tables
    
    def get_expected_table_names(self, stat_types: List[str]) -> List[str]:
        """
        Get expected table names for given stat types
        
        Args:
            stat_types: List of stat types like ['standard', 'passing']
            
        Returns:
            List of expected table names like ['squad_standard', 'opponent_standard', 'player_standard', ...]
        """
        expected_tables = []
        
        for stat_type in stat_types:
            if stat_type in self.config['stats_sources']:
                expected_tables.extend(self.config['stats_sources'][stat_type]['tables'])
        
        return expected_tables
    
    def validate_scraped_data(self, scraped_tables: Dict[str, pd.DataFrame], 
                            expected_stat_types: List[str]) -> Dict[str, str]:
        """
        Validate that we got the expected tables
        
        Returns:
            Dict mapping table_name -> status ('success' or error message)
        """
        expected_tables = self.get_expected_table_names(expected_stat_types)
        results = {}
        
        for table_name in expected_tables:
            if table_name in scraped_tables:
                df = scraped_tables[table_name]
                if len(df) > 0:
                    results[table_name] = 'success'
                else:
                    results[table_name] = 'empty_dataframe'
            else:
                results[table_name] = 'missing_table'
        
        # Check for unexpected tables
        for table_name in scraped_tables:
            if table_name not in expected_tables:
                results[table_name] = 'unexpected_table'
        
        return results
    
    def scrape_from_config_mapping(self, file_mapping: Dict[str, str]) -> Dict[str, pd.DataFrame]:
        """
        Convenience method: scrape based on stat_type -> file_path mapping
        
        Args:
            file_mapping: {'standard': 'data/standard.html', 'passing': 'data/passing.html'}
        """
        return self.scrape_multiple_categories(file_mapping)