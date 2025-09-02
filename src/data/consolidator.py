"""
Data Consolidator - Merges 11 FBRef categories into consolidated tables
Takes scraped data and combines related stats into wider tables
"""
import pandas as pd
import logging
from typing import Dict, List

logger = logging.getLogger(__name__)

class DataConsolidator:
    """
    Consolidates 11 FBRef stat categories into 3 comprehensive tables
    """
    
    def __init__(self):
        # Define which FBRef categories map to each consolidated table
        self.stat_category_mapping = {
            'player': [
                'standard', 'keepers', 'keepersadv', 'shooting', 'passing', 
                'passingtypes', 'goalshotcreation', 'defense', 'possession', 
                'playingtime', 'misc'
            ],
            'squad': [
                'standard', 'keepers', 'keepersadv', 'shooting', 'passing',
                'passingtypes', 'goalshotcreation', 'defense', 'possession',
                'playingtime', 'misc'
            ],
            'opponent': [
                'standard', 'keepers', 'keepersadv', 'shooting', 'passing',
                'passingtypes', 'goalshotcreation', 'defense', 'possession',
                'playingtime', 'misc'
            ]
        }
    
    def consolidate_all_stats(self, scraped_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Consolidate all scraped data into 3 comprehensive tables
        
        Args:
            scraped_data: Dict with keys like 'player_standard', 'squad_passing', etc.
            
        Returns:
            Dict with keys: 'current_player_stats', 'current_squad_stats', 'current_opponent_stats'
        """
        logger.info("Starting data consolidation...")
        
        consolidated = {}
        
        # Consolidate player stats
        player_tables = self._extract_category_tables(scraped_data, 'player')
        if player_tables:
            consolidated['current_player_stats'] = self._merge_tables(player_tables, 'player')
            logger.info(f"Consolidated player stats: {consolidated['current_player_stats'].shape}")
        
        # Consolidate squad stats  
        squad_tables = self._extract_category_tables(scraped_data, 'squad')
        if squad_tables:
            consolidated['current_squad_stats'] = self._merge_tables(squad_tables, 'squad')
            logger.info(f"Consolidated squad stats: {consolidated['current_squad_stats'].shape}")
        
        # Consolidate opponent stats
        opponent_tables = self._extract_category_tables(scraped_data, 'opponent')
        if opponent_tables:
            consolidated['current_opponent_stats'] = self._merge_tables(opponent_tables, 'opponent')
            logger.info(f"Consolidated opponent stats: {consolidated['current_opponent_stats'].shape}")
        
        logger.info(f"Data consolidation complete: {len(consolidated)} tables created")
        return consolidated
    
    def _extract_category_tables(self, scraped_data: Dict[str, pd.DataFrame], 
                                category: str) -> Dict[str, pd.DataFrame]:
        """Extract all tables for a specific category (player/squad/opponent)"""
        category_tables = {}
        
        for stat_type in self.stat_category_mapping[category]:
            table_name = f"{category}_{stat_type}"
            if table_name in scraped_data:
                category_tables[stat_type] = scraped_data[table_name]
        
        logger.debug(f"Found {len(category_tables)} tables for {category}: {list(category_tables.keys())}")
        return category_tables
    
    def _merge_tables(self, tables: Dict[str, pd.DataFrame], category: str) -> pd.DataFrame:
        """
        Merge multiple stat tables into one comprehensive table
        
        Args:
            tables: Dict of DataFrames to merge (e.g., {'standard': df1, 'passing': df2})
            category: 'player', 'squad', or 'opponent'
        """
        if not tables:
            logger.warning(f"No tables to merge for {category}")
            return pd.DataFrame()
        
        # Start with the first table
        first_table_name = list(tables.keys())[0]
        merged_df = tables[first_table_name].copy()
        
        logger.info(f"Starting merge with {first_table_name}: {merged_df.shape}")
        
        # Define the key columns for joining based on category
        if category == 'player':
            key_columns = ['Player', 'Nation', 'Pos', 'Squad', 'Age']
        else:  # squad or opponent
            key_columns = ['Squad']
        
        # Merge each additional table
        for table_name, df in list(tables.items())[1:]:  # Skip first table
            logger.debug(f"Merging {table_name}: {df.shape}")
            
            # Get columns to add (exclude key columns and metadata columns)
            existing_cols = set(merged_df.columns)
            metadata_cols = {'gameweek', 'scraped_date', 'current_date'}
            
            new_cols = [col for col in df.columns 
                       if col not in key_columns 
                       and col not in existing_cols
                       and col not in metadata_cols]
            
            if new_cols:
                # Select key columns + new columns for merge
                merge_columns = key_columns + new_cols
                df_to_merge = df[merge_columns]
                
                # Merge on key columns
                merged_df = merged_df.merge(df_to_merge, on=key_columns, how='left')
                logger.debug(f"Added {len(new_cols)} columns from {table_name}")
            else:
                logger.debug(f"No new columns to add from {table_name}")
        
        logger.info(f"Final merged table for {category}: {merged_df.shape}")
        return merged_df
    
    def get_expected_tables(self) -> List[str]:
        """Get list of expected consolidated table names"""
        return ['current_player_stats', 'current_squad_stats', 'current_opponent_stats']
    
    def validate_consolidation(self, original_data: Dict[str, pd.DataFrame], 
                             consolidated_data: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """
        Validate that consolidation worked correctly
        
        Returns:
            Dict with validation results
        """
        results = {}
        
        for table_name in self.get_expected_tables():
            if table_name in consolidated_data:
                df = consolidated_data[table_name]
                if len(df) > 0:
                    results[table_name] = f"success: {df.shape[0]} rows, {df.shape[1]} columns"
                else:
                    results[table_name] = "warning: empty table"
            else:
                results[table_name] = "error: missing table"
        
        return results