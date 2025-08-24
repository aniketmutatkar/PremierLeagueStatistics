"""
Stats Scraper - Identifies the right tables and cleans them
Uses your exact cleanData() logic from archive_v1/src/datacleaner.py
"""
import pandas as pd
import logging
from datetime import datetime
from typing import Dict, List, Tuple
from .base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class StatsScraper(BaseScraper):
    """
    Stats scraper that identifies the right tables from FBRef
    and cleans them using your working logic
    """
    
    def scrape_stats(self, source) -> Dict[str, pd.DataFrame]:
        """
        Scrape and process stats tables
        
        Returns:
            Dictionary with keys: squad_stats, opponent_stats, player_stats
        """
        logger.info(f"Starting stats scrape of {source}")
        
        # Get all tables using base scraper
        all_tables = self.scrape_tables(source)
        
        if not all_tables:
            logger.error("No tables extracted")
            return {}
        
        # Identify the right tables (from your test: tables 9, 10, 11)
        squad_table, opponent_table, player_table = self._identify_stat_tables(all_tables)
        
        if squad_table is None or opponent_table is None or player_table is None:
            logger.error("Could not identify all required stat tables")
            return {}
        
        # Clean the data using your exact logic (now type-safe)
        cleaned_tables = self._clean_data([squad_table, opponent_table, player_table])
        
        return {
            'squad_stats': cleaned_tables[0],
            'opponent_stats': cleaned_tables[1], 
            'player_stats': cleaned_tables[2]
        }
    
    def _identify_stat_tables(self, tables: List[pd.DataFrame]) -> Tuple[pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None]:
        """
        Identify which tables are squad, opponent, and player stats
        Based on your test results, we need tables with proper FBRef structure
        """
        squad_table = None
        opponent_table = None
        player_table = None
        
        for i, table in enumerate(tables):
            # Skip small junk tables (less than 15 rows)
            if len(table) < 15:
                continue
                
            # Look for table structure indicators
            columns = [str(col).lower() for col in table.columns]
            
            # Player table: has 'player', 'nation', 'pos' columns and 300+ rows
            if any('player' in col for col in columns) and len(table) > 250:
                logger.info(f"Identified player table: Table {i} with {len(table)} rows")
                player_table = table
                
            # Squad table: has 'squad' column and ~20 rows (Premier League teams)
            elif any('squad' in col for col in columns) and 15 <= len(table) <= 25:
                # Check if it's opponent stats (contains "vs" in squad names)
                first_squad_col = None
                for col in table.columns:
                    if 'squad' in str(col).lower():
                        first_squad_col = col
                        break
                
                if first_squad_col is not None:
                    sample_squads = table[first_squad_col].dropna().astype(str).head(3).tolist()
                    if any('vs ' in squad for squad in sample_squads):
                        logger.info(f"Identified opponent table: Table {i} with {len(table)} rows")
                        opponent_table = table
                    else:
                        logger.info(f"Identified squad table: Table {i} with {len(table)} rows") 
                        squad_table = table
        
        return squad_table, opponent_table, player_table
    
    def _clean_data(self, rawdata: List[pd.DataFrame]) -> List[pd.DataFrame]:
        """
        Your exact cleanData() logic from archive_v1/src/datacleaner.py
        """
        logger.info("Starting data cleaning process")
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        if len(rawdata) < 3:
            raise ValueError("Expected at least three DataFrames")

        cleaned_tables = []
        
        for i, df in enumerate(rawdata[:3]):  # Only process first 3 tables
            logger.debug(f"Cleaning DataFrame {i+1}/3")
            
            # Your existing column cleaning logic
            df = df.copy()
            df.columns = [' '.join(col).strip() for col in df.columns]
            df = df.reset_index(drop=True)
            
            # Clean column names (your logic)
            new_columns = []
            for cols in df.columns:
                if 'level_0' in cols:
                    new_col = cols.split()[-1]  # takes the last name
                else:
                    new_col = cols
                new_columns.append(new_col)
            
            df.columns = new_columns
            df = df.fillna(0)

            # Add a column with the current date
            df['current_date'] = current_date
            
            cleaned_tables.append(df)

        # Rename DataFrames (your logic)
        squad_stats = cleaned_tables[0]
        opponent_stats = cleaned_tables[1]
        player_stats = cleaned_tables[2]

        # Format Columns in Player Standard Stats DataFrame (your logic)
        if 'Age' in player_stats.columns:
            player_stats['Age'] = player_stats['Age'].str[:2]
        if 'Nation' in player_stats.columns:
            player_stats['Nation'] = player_stats['Nation'].str.split(' ').str.get(1)
        if 'Rk' in player_stats.columns:
            player_stats = player_stats.drop(columns=['Rk'], errors='ignore')
        if 'Matches' in player_stats.columns:
            player_stats = player_stats.drop(columns=['Matches'], errors='ignore')

        # Drop all the rows that have NaN in the row
        initial_player_count = len(player_stats)
        player_stats.dropna(inplace=True)
        final_player_count = len(player_stats)
        
        logger.info(f"Dropped {initial_player_count - final_player_count} rows with NaN values from PlayerStats.")

        # Convert all the Data types of the numeric columns from object to numeric (your logic)
        for col in squad_stats.columns[1:-1]:
            squad_stats[col] = pd.to_numeric(squad_stats[col], errors='coerce')
        for col in opponent_stats.columns[1:-1]:
            opponent_stats[col] = pd.to_numeric(opponent_stats[col], errors='coerce')
        for col in player_stats.columns[4:-1]:
            player_stats[col] = pd.to_numeric(player_stats[col], errors='coerce')

        logger.info("Data cleaning process completed successfully")
        
        return [squad_stats, opponent_stats, player_stats]