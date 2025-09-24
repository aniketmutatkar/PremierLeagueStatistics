"""
Unified Data Consolidator - Built from scratch for all entity types
Handles players, squads, and opponents using the same patterns

Designed from the ground up to be entity-aware while maintaining
all the error handling, validation, and logging from the original.
"""

import pandas as pd
import logging
from typing import Dict, List, Optional, Tuple
import hashlib
from datetime import datetime
from src.scraping.fbref_scraper import FBRefScraper
from .column_mappings import get_entity_mappings, EXCLUDED_COLUMNS

logger = logging.getLogger(__name__)

class DataConsolidator:
    """
    Unified data consolidation for players, squads, and opponents
    
    Entity-aware consolidation using explicit column mappings
    Maintains all error handling and validation from original player consolidator
    """
    
    def __init__(self):
        # Entity table definitions
        self.entity_tables = {
            'player': [
                'player_standard', 'player_shooting', 'player_passing', 'player_passingtypes',
                'player_goalshotcreation', 'player_defense', 'player_possession', 
                'player_misc', 'player_keepers', 'player_keepersadv'
            ],
            'squad': [
                'squad_standard', 'squad_shooting', 'squad_passing', 'squad_passingtypes',
                'squad_goalshotcreation', 'squad_defense', 'squad_possession',
                'squad_misc', 'squad_keepers', 'squad_keepersadv'
            ],
            'opponent': [
                'opponent_standard', 'opponent_shooting', 'opponent_passing', 'opponent_passingtypes',
                'opponent_goalshotcreation', 'opponent_defense', 'opponent_possession',
                'opponent_misc', 'opponent_keepers', 'opponent_keepersadv'
            ]
        }
        
        # Player-specific table separation (only applies to players)
        self.player_outfield_tables = [
            'player_standard', 'player_shooting', 'player_passing', 'player_passingtypes',
            'player_goalshotcreation', 'player_defense', 'player_possession', 'player_misc'
        ]
        
        self.player_goalkeeper_tables = [
            'player_standard', 'player_keepers', 'player_keepersadv'
        ]
    
    # =====================================================
    # PUBLIC METHODS - Entity-specific interfaces
    # =====================================================
    
    def consolidate_players(self, raw_conn, gameweek: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Consolidate player data for both outfield players and goalkeepers
        
        Args:
            raw_conn: Connection to raw database
            gameweek: Target gameweek to consolidate
            
        Returns:
            Tuple of (outfield_df, goalkeepers_df)
        """
        logger.info(f"Starting player consolidation for gameweek {gameweek}")
        
        # Get base players from player_standard
        all_players_df = self._get_base_entity_data(raw_conn, 'player', gameweek)
        if all_players_df.empty:
            logger.error("No players found in player_standard")
            return pd.DataFrame(), pd.DataFrame()
        
        # Separate players by position (only for players)
        is_goalkeeper = all_players_df['Pos'].str.contains('GK', na=False)
        outfield_players = all_players_df[~is_goalkeeper].copy()
        goalkeepers = all_players_df[is_goalkeeper].copy()
        
        logger.info(f"Found {len(outfield_players)} outfield players and {len(goalkeepers)} goalkeepers")
        
        # Consolidate outfield players
        outfield_df = self._consolidate_entity_data(
            raw_conn, outfield_players, self.player_outfield_tables, gameweek, 'player', 'outfield'
        )
        
        # Consolidate goalkeepers
        goalkeepers_df = self._consolidate_entity_data(
            raw_conn, goalkeepers, self.player_goalkeeper_tables, gameweek, 'player', 'goalkeeper'
        )
        
        logger.info(f"Player consolidation complete: {len(outfield_df)} outfield, {len(goalkeepers_df)} goalkeepers")
        return outfield_df, goalkeepers_df
    
    def consolidate_squads(self, raw_conn, gameweek: int) -> pd.DataFrame:
        """
        Consolidate squad data from all 11 squad tables
        
        Args:
            raw_conn: Connection to raw database
            gameweek: Target gameweek to consolidate
            
        Returns:
            Consolidated squad DataFrame
        """
        logger.info(f"Starting squad consolidation for gameweek {gameweek}")
        
        # Get base squad data
        base_squads_df = self._get_base_entity_data(raw_conn, 'squad', gameweek)
        if base_squads_df.empty:
            logger.error("No squads found in squad_standard")
            return pd.DataFrame()
        
        # Consolidate all squad tables (no position separation)
        squad_df = self._consolidate_entity_data(
            raw_conn, base_squads_df, self.entity_tables['squad'], gameweek, 'squad'
        )
        
        logger.info(f"Squad consolidation complete: {len(squad_df)} squads")
        return squad_df
    
    def consolidate_opponents(self, raw_conn, gameweek: int) -> pd.DataFrame:
        """
        Consolidate opponent data from all 11 opponent tables
        
        Args:
            raw_conn: Connection to raw database
            gameweek: Target gameweek to consolidate
            
        Returns:
            Consolidated opponent DataFrame
        """
        logger.info(f"Starting opponent consolidation for gameweek {gameweek}")
        
        # Get base opponent data
        base_opponents_df = self._get_base_entity_data(raw_conn, 'opponent', gameweek)
        if base_opponents_df.empty:
            logger.error("No opponents found in opponent_standard")
            return pd.DataFrame()
        
        # Consolidate all opponent tables (no position separation)
        opponent_df = self._consolidate_entity_data(
            raw_conn, base_opponents_df, self.entity_tables['opponent'], gameweek, 'opponent'
        )
        
        logger.info(f"Opponent consolidation complete: {len(opponent_df)} opponents")
        return opponent_df
    
    # =====================================================
    # CORE PRIVATE METHODS - Entity-aware implementations
    # =====================================================
    
    def _get_base_entity_data(self, raw_conn, entity_type: str, gameweek: int) -> pd.DataFrame:
        """Get base entity data from {entity}_standard table"""
        base_table = f"{entity_type}_standard"
        
        try:
            query = f"""
            SELECT * FROM {base_table} 
            WHERE current_through_gameweek = ?
            """
            
            df = pd.read_sql(query, raw_conn, params=[gameweek])
            
            if not df.empty:
                # Create entity key for joining (entity-specific logic)
                df = self._create_entity_key(df, entity_type)
                
                # Remove duplicates
                key_col = self._get_key_column(entity_type)
                initial_count = len(df)
                df = df.drop_duplicates(subset=[key_col], keep='first')
                final_count = len(df)
                
                if initial_count != final_count:
                    logger.warning(f"Removed {initial_count - final_count} duplicate {entity_type}s from base data")
            
            logger.info(f"Loaded {len(df)} total {entity_type}s from {base_table}")
            return df
            
        except Exception as e:
            logger.error(f"Error loading base {entity_type} data: {e}")
            return pd.DataFrame()
    
    def _consolidate_entity_data(self, raw_conn, base_df: pd.DataFrame, tables: List[str], 
                                gameweek: int, entity_type: str, player_type: Optional[str] = None) -> pd.DataFrame:
        """
        Core consolidation logic for any entity type
        
        This is the unified method that handles all entity types using the same patterns
        """
        logger.info(f"Consolidating {len(base_df)} {entity_type}s using {len(tables)} tables")
        
        # Apply mappings to base table
        base_table = tables[0]
        result_df = self._apply_table_mappings(base_df, base_table, entity_type, player_type)
        
        # Process each additional table
        for table_name in tables[1:]:  # Skip base table (already processed)
            try:
                # Get raw data from this table
                table_df = self._get_table_data(raw_conn, table_name, gameweek, entity_type)
                
                if table_df.empty:
                    logger.warning(f"No data found in {table_name} for gameweek {gameweek}")
                    continue
                
                # Apply mappings to get analytics column names
                mapped_df = self._apply_table_mappings(table_df, table_name, entity_type, player_type)
                
                # Merge with main result
                result_df = self._merge_table_data(result_df, mapped_df, table_name, entity_type)
                
                logger.debug(f"Successfully processed {table_name}: {len(mapped_df)} records")
                
            except Exception as e:
                logger.error(f"Error processing {table_name}: {e}")
                # Continue with other tables even if one fails
                continue
        
        # Add SCD Type 2 metadata
        result_df = self._add_scd_metadata(result_df, gameweek, entity_type)
        
        logger.info(f"{entity_type.title()} consolidation complete: {len(result_df)} entities, {len(result_df.columns)} columns")
        return result_df
    
    def _get_table_data(self, raw_conn, table_name: str, gameweek: int, entity_type: str) -> pd.DataFrame:
        """Get data from a specific stat table for any entity type"""
        try:
            query = f"""
            SELECT * FROM {table_name} 
            WHERE current_through_gameweek = ?
            """
            
            df = pd.read_sql(query, raw_conn, params=[gameweek])
            
            if not df.empty:
                # Create entity key for joining
                df = self._create_entity_key(df, entity_type)
                
                # Remove duplicates
                key_col = self._get_key_column(entity_type)
                initial_count = len(df)
                df = df.drop_duplicates(subset=[key_col], keep='first')
                final_count = len(df)
                
                if initial_count != final_count:
                    logger.warning(f"Removed {initial_count - final_count} duplicates from {table_name}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading data from {table_name}: {e}")
            return pd.DataFrame()
    
    def _apply_table_mappings(self, df: pd.DataFrame, table_name: str, entity_type: str, 
                             player_type: Optional[str] = None) -> pd.DataFrame:
        """
        Apply explicit column mappings to transform raw columns to analytics columns
        
        Entity-aware version that maintains all original error handling and validation
        """
        if df.empty:
            return df
        
        # Get the appropriate mapping dictionary based on entity type
        if entity_type == 'player':
            mappings_dict = get_entity_mappings('player', player_type)
        else:
            mappings_dict = get_entity_mappings(entity_type)
        
        # Get mappings for this specific table
        if table_name not in mappings_dict:
            logger.warning(f"No mappings defined for {table_name} in {entity_type} mappings")
            key_col = self._get_key_column(entity_type)
            return df[[key_col]].copy()
        
        table_mappings = mappings_dict[table_name]
        
        # Build rename dictionary for columns that exist and are mapped
        rename_dict = {}
        unmapped_columns = []
        missing_columns = []
        
        for raw_col, analytics_col in table_mappings.items():
            if raw_col in df.columns:
                rename_dict[raw_col] = analytics_col
            else:
                missing_columns.append(raw_col)
        
        # Check for unmapped statistical columns (excluding metadata and basic info)
        key_col = self._get_key_column(entity_type)
        for col in df.columns:
            if (col not in table_mappings and 
                col not in EXCLUDED_COLUMNS and 
                col != key_col):
                unmapped_columns.append(col)
        
        # Log any issues (same detailed logging as original)
        if missing_columns:
            logger.warning(f"Expected columns missing from {table_name}: {missing_columns}")
        
        if unmapped_columns:
            logger.info(f"Unmapped columns in {table_name}: {unmapped_columns}")
        
        # Apply the mapping
        columns_to_keep = [key_col] + list(rename_dict.keys())
        mapped_df = df[columns_to_keep].copy()
        mapped_df = mapped_df.rename(columns=rename_dict)
        
        logger.debug(f"Applied mappings to {table_name}: {len(rename_dict)} columns mapped")
        return mapped_df
    
    def _merge_table_data(self, main_df: pd.DataFrame, new_df: pd.DataFrame, table_name: str, entity_type: str) -> pd.DataFrame:
        """Merge new table data with main dataframe - entity-aware version"""
        try:
            key_col = self._get_key_column(entity_type)
            
            # Perform left join on entity key
            merged_df = main_df.merge(new_df, on=key_col, how='left')
            
            # Check for any column conflicts (same detailed checking as original)
            original_cols = set(main_df.columns)
            new_cols = set(new_df.columns) - {key_col}
            conflicts = original_cols & new_cols
            
            if conflicts:
                logger.error(f"Column conflicts detected when merging {table_name}: {conflicts}")
                # This should not happen with explicit mappings!
            
            logger.debug(f"Merged {table_name}: added {len(new_cols)} columns")
            return merged_df
            
        except Exception as e:
            logger.error(f"Error merging {table_name}: {e}")
            return main_df  # Return original if merge fails
    
    def _add_scd_metadata(self, df: pd.DataFrame, gameweek: int, entity_type: str) -> pd.DataFrame:
        """Add SCD Type 2 metadata columns - entity-aware version"""
        if df.empty:
            return df
        
        # Initialize Scraper to add to business key
        scraper = FBRefScraper()
        df['season'] = "2023-2024" # scraper._extract_season_info()

        # Create entity_id (business key) based on entity type
        if entity_type == 'player':
            df['player_id'] = df['player_name'] + '_' + df['born_year'].astype(str) + '_' + df['squad'] + '_' + df['season']
        else:  # squad or opponent
            df['entity_id'] = df['squad_name'] + '_' + df['season']
        
        # Create unique analytics key (hash of entity_id + gameweek)
        def generate_analytics_key(row):
            if entity_type == 'player':
                business_key = row['player_id']
            else:
                business_key = row['entity_id']

            key_string = f"{business_key}_{gameweek}"
            return int(hashlib.md5(key_string.encode()).hexdigest()[:12], 16)

        # Set the appropriate key column based on entity type
        key_col = self._get_final_key_column(entity_type)
        df[key_col] = df.apply(generate_analytics_key, axis=1)
        
        # Add SCD Type 2 metadata (same for all entity types)
        df['gameweek'] = gameweek
        df['valid_from'] = datetime.now().date()
        df['valid_to'] = None
        df['is_current'] = True
        
        df = df.drop(columns=['entity_key'], errors='ignore')

        return df
    
    # =====================================================
    # UTILITY METHODS - Entity-aware helpers
    # =====================================================
    
    def _create_entity_key(self, df: pd.DataFrame, entity_type: str) -> pd.DataFrame:
        """Create appropriate entity key for joining based on entity type"""
        if entity_type == 'player':
            df['entity_key'] = df['Player'] + '_' + df['Born'].astype(str) + '_' + df['Squad']
        else:  # squad or opponent
            df['entity_key'] = df['Squad']
        return df
    
    def _get_key_column(self, entity_type: str) -> str:
        """Get the temporary key column name used during processing"""
        return 'entity_key'  # Same for all during processing
    
    def _get_final_key_column(self, entity_type: str) -> str:
        """Get the final key column name in the analytics table"""
        if entity_type == 'player':
            return 'player_key'
        elif entity_type == 'squad':
            return 'squad_key'
        elif entity_type == 'opponent':
            return 'opponent_key'
    
    # =====================================================
    # VALIDATION METHODS - Extended for all entity types
    # =====================================================
    
    def get_consolidation_summary(self, **dataframes) -> Dict:
        """
        Get summary statistics about consolidated data for any entity types
        
        Usage: 
        summary = consolidator.get_consolidation_summary(
            outfield=outfield_df, goalkeepers=goalkeepers_df,
            squads=squad_df, opponents=opponent_df
        )
        """
        summary = {}
        
        for entity_name, df in dataframes.items():
            if df is not None and not df.empty:
                summary[f"{entity_name}_count"] = len(df)
                summary[f"{entity_name}_columns"] = len(df.columns)
                summary[f"{entity_name}_missing_values"] = df.isnull().sum().sum()
                
                # Entity-specific stats
                if 'squad_name' in df.columns:
                    summary[f"{entity_name}_unique_squads"] = df['squad_name'].nunique()
                if 'goals' in df.columns:
                    summary[f"{entity_name}_total_goals"] = df['goals'].sum()
            else:
                summary[f"{entity_name}_count"] = 0
                summary[f"{entity_name}_columns"] = 0
        
        # Calculate totals
        total_entities = sum(v for k, v in summary.items() if k.endswith('_count'))
        summary['total_entities'] = total_entities
        
        return summary
    
    def validate_consolidation(self, **dataframes) -> Dict:
        """
        Validate consolidation results for any entity types
        
        Usage:
        validation = consolidator.validate_consolidation(
            outfield=outfield_df, goalkeepers=goalkeepers_df,
            squads=squad_df, opponents=opponent_df
        )
        """
        validation_results = {
            'success': True,
            'errors': [],
            'warnings': []
        }
        
        for entity_name, df in dataframes.items():
            if df is None or df.empty:
                validation_results['warnings'].append(f"No {entity_name} data consolidated")
                continue
            
            # Check for required key column
            if 'player' in entity_name and 'player_key' not in df.columns:
                validation_results['errors'].append(f"Missing player_key in {entity_name}")
                validation_results['success'] = False
            elif 'squad' in entity_name and 'squad_key' not in df.columns:
                validation_results['errors'].append(f"Missing squad_key in {entity_name}")
                validation_results['success'] = False
            elif 'opponent' in entity_name and 'opponent_key' not in df.columns:
                validation_results['errors'].append(f"Missing opponent_key in {entity_name}")
                validation_results['success'] = False
            
            # Check for basic required columns
            required_cols = ['gameweek', 'season', 'is_current']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                validation_results['errors'].append(f"Missing required columns in {entity_name}: {missing_cols}")
                validation_results['success'] = False
        
        return validation_results