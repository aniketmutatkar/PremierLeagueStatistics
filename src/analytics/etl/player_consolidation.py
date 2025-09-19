"""
Player Data Consolidation - REBUILT FROM SCRATCH
Clean implementation using explicit column mappings

NO PREFIXES. NO BROKEN MAPPINGS. NO FUCKSHIT.
Just clean, explicit mapping from raw FBRef columns to analytics columns.
"""

import pandas as pd
import logging
from typing import Dict, List, Optional, Tuple
import hashlib
from datetime import datetime

# Import our explicit mapping dictionary
from .column_mappings import (
    OUTFIELD_PLAYER_MAPPINGS,
    GOALKEEPER_MAPPINGS,
    COLUMN_PRIORITIES,
    EXCLUDED_COLUMNS
)

logger = logging.getLogger(__name__)

class PlayerDataConsolidator:
    """
    Clean player data consolidation using explicit column mappings
    
    Separates outfield players and goalkeepers into different flows
    Maps raw FBRef columns directly to analytics column names
    No prefixes, no broken mappings, no unnecessary complexity
    """
    
    def __init__(self):
        # Define all 11 player stat tables in processing order
        self.player_tables = [
            'player_standard',      # PRIMARY - base for all players
            'player_shooting',      # Outfield only - shooting stats
            'player_passing',       # Outfield only - passing stats  
            'player_passingtypes',  # Outfield only - pass types
            'player_goalshotcreation', # Outfield only - creation stats
            'player_defense',       # Outfield only - defensive stats
            'player_possession',    # Outfield only - possession stats
            'player_misc',          # Outfield only - miscellaneous stats
            'player_keepers',       # Goalkeepers only - basic keeper stats
            'player_keepersadv'     # Goalkeepers only - advanced keeper stats
        ]
        
        # Tables that apply to outfield players only
        self.outfield_tables = [
            'player_standard', 'player_shooting', 'player_passing',
            'player_passingtypes', 'player_goalshotcreation', 
            'player_defense', 'player_possession', 'player_misc'
        ]
        
        # Tables that apply to goalkeepers only
        self.goalkeeper_tables = [
            'player_standard', 'player_keepers', 'player_keepersadv'
        ]
    
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
        
        # Step 1: Get all players from player_standard (the base table)
        all_players_df = self._get_base_players(raw_conn, gameweek)
        if all_players_df.empty:
            logger.error("No players found in player_standard")
            return pd.DataFrame(), pd.DataFrame()
        
        # Step 2: Separate players by position
        # Handle multi-position players (e.g., 'MF,FW', 'DF,MF') 
        is_goalkeeper = all_players_df['Pos'].str.contains('GK', na=False)
        outfield_players = all_players_df[~is_goalkeeper].copy()
        goalkeepers = all_players_df[is_goalkeeper].copy()
        
        logger.info(f"Found {len(outfield_players)} outfield players and {len(goalkeepers)} goalkeepers")
        
        # Step 3: Process outfield players
        outfield_df = self._consolidate_outfield_players(raw_conn, outfield_players, gameweek)
        
        # Step 4: Process goalkeepers
        goalkeepers_df = self._consolidate_goalkeepers(raw_conn, goalkeepers, gameweek)
        
        return outfield_df, goalkeepers_df
    
    def _get_base_players(self, raw_conn, gameweek: int) -> pd.DataFrame:
        """Get all players from player_standard as the base"""
        try:
            query = """
            SELECT * FROM player_standard 
            WHERE current_through_gameweek = ?
            """
            
            df = pd.read_sql(query, raw_conn, params=[gameweek])
            
            if not df.empty:
                # Create player_key for joining (player_name + born + squad)
                df['player_key'] = df['Player'] + '_' + df['Born'].astype(str) + '_' + df['Squad']
                
                # Remove any duplicates
                initial_count = len(df)
                df = df.drop_duplicates(subset=['player_key'], keep='first')
                final_count = len(df)
                
                if initial_count != final_count:
                    logger.warning(f"Removed {initial_count - final_count} duplicate players from base data")
            
            logger.info(f"Loaded {len(df)} total players from player_standard")
            return df
            
        except Exception as e:
            logger.error(f"Error loading base player data: {e}")
            return pd.DataFrame()
    
    def _consolidate_outfield_players(self, raw_conn, base_players: pd.DataFrame, gameweek: int) -> pd.DataFrame:
        """Consolidate data for outfield players (DF, MF, FW)"""
        logger.info(f"Consolidating {len(base_players)} outfield players")
        
        # Start with base players and apply standard table mappings
        result_df = self._apply_table_mappings(
            base_players, 'player_standard', 'outfield'
        )
        
        # Process each additional outfield table
        for table_name in self.outfield_tables[1:]:  # Skip player_standard (already processed)
            try:
                # Get raw data from this table
                table_df = self._get_table_data(raw_conn, table_name, gameweek)
                
                if table_df.empty:
                    logger.warning(f"No data found in {table_name} for gameweek {gameweek}")
                    continue
                
                # Apply mappings to get analytics column names
                mapped_df = self._apply_table_mappings(table_df, table_name, 'outfield')
                
                # Merge with main result
                result_df = self._merge_table_data(result_df, mapped_df, table_name)
                
                logger.debug(f"Successfully processed {table_name}: {len(mapped_df)} records")
                
            except Exception as e:
                logger.error(f"Error processing {table_name}: {e}")
                # Continue with other tables even if one fails
                continue
        
        # Add SCD Type 2 metadata
        result_df = self._add_scd_metadata(result_df, gameweek)
        
        logger.info(f"Outfield consolidation complete: {len(result_df)} players, {len(result_df.columns)} columns")
        return result_df
    
    def _consolidate_goalkeepers(self, raw_conn, base_goalkeepers: pd.DataFrame, gameweek: int) -> pd.DataFrame:
        """Consolidate data for goalkeepers (GK)"""
        logger.info(f"Consolidating {len(base_goalkeepers)} goalkeepers")
        
        # Start with base goalkeepers and apply standard table mappings
        result_df = self._apply_table_mappings(
            base_goalkeepers, 'player_standard', 'goalkeeper'
        )
        
        # Process each goalkeeper-specific table
        for table_name in self.goalkeeper_tables[1:]:  # Skip player_standard (already processed)
            try:
                # Get raw data from this table
                table_df = self._get_table_data(raw_conn, table_name, gameweek)
                
                if table_df.empty:
                    logger.warning(f"No data found in {table_name} for gameweek {gameweek}")
                    continue
                
                # Apply mappings to get analytics column names
                mapped_df = self._apply_table_mappings(table_df, table_name, 'goalkeeper')
                
                # Merge with main result
                result_df = self._merge_table_data(result_df, mapped_df, table_name)
                
                logger.debug(f"Successfully processed {table_name}: {len(mapped_df)} records")
                
            except Exception as e:
                logger.error(f"Error processing {table_name}: {e}")
                # Continue with other tables even if one fails
                continue
        
        # Add SCD Type 2 metadata
        result_df = self._add_scd_metadata(result_df, gameweek)
        
        logger.info(f"Goalkeeper consolidation complete: {len(result_df)} players, {len(result_df.columns)} columns")
        return result_df
    
    def _get_table_data(self, raw_conn, table_name: str, gameweek: int) -> pd.DataFrame:
        """Get data from a specific player stat table"""
        try:
            query = f"""
            SELECT * FROM {table_name} 
            WHERE current_through_gameweek = ?
            """
            
            df = pd.read_sql(query, raw_conn, params=[gameweek])
            
            if not df.empty:
                # Create player_key for joining
                df['player_key'] = df['Player'] + '_' + df['Born'].astype(str) + '_' + df['Squad']
                
                # Remove duplicates
                initial_count = len(df)
                df = df.drop_duplicates(subset=['player_key'], keep='first')
                final_count = len(df)
                
                if initial_count != final_count:
                    logger.warning(f"Removed {initial_count - final_count} duplicates from {table_name}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading data from {table_name}: {e}")
            return pd.DataFrame()
    
    def _apply_table_mappings(self, df: pd.DataFrame, table_name: str, player_type: str) -> pd.DataFrame:
        """
        Apply explicit column mappings to transform raw columns to analytics columns
        
        This is the core function that replaces the broken prefix system
        """
        if df.empty:
            return df
        
        # Get the appropriate mapping dictionary
        if player_type == 'outfield':
            mappings_dict = OUTFIELD_PLAYER_MAPPINGS
        else:
            mappings_dict = GOALKEEPER_MAPPINGS
        
        # Get mappings for this specific table
        if table_name not in mappings_dict:
            logger.warning(f"No mappings defined for {table_name} in {player_type} mappings")
            return df[['player_key']].copy()  # Return empty with just player_key
        
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
        for col in df.columns:
            if (col not in table_mappings and 
                col not in EXCLUDED_COLUMNS and 
                col != 'player_key'):
                unmapped_columns.append(col)
        
        # Log any issues
        if missing_columns:
            logger.warning(f"Expected columns missing from {table_name}: {missing_columns}")
        
        if unmapped_columns:
            logger.info(f"Unmapped columns in {table_name}: {unmapped_columns}")
        
        # Apply the mapping
        columns_to_keep = ['player_key'] + list(rename_dict.keys())
        mapped_df = df[columns_to_keep].copy()
        mapped_df = mapped_df.rename(columns=rename_dict)
        
        logger.debug(f"Applied mappings to {table_name}: {len(rename_dict)} columns mapped")
        return mapped_df
    
    def _merge_table_data(self, main_df: pd.DataFrame, new_df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Merge new table data with main dataframe"""
        try:
            # Perform left join on player_key
            merged_df = main_df.merge(new_df, on='player_key', how='left')
            
            # Check for any column conflicts (shouldn't happen with our explicit mappings)
            original_cols = set(main_df.columns)
            new_cols = set(new_df.columns) - {'player_key'}
            conflicts = original_cols & new_cols
            
            if conflicts:
                logger.error(f"Column conflicts detected when merging {table_name}: {conflicts}")
                # This should not happen with our explicit mappings!
                # If it does, we have a bug in our mapping dictionary
            
            logger.debug(f"Merged {table_name}: added {len(new_cols)} columns")
            return merged_df
            
        except Exception as e:
            logger.error(f"Error merging {table_name}: {e}")
            return main_df  # Return original if merge fails
    
    def _add_scd_metadata(self, df: pd.DataFrame, gameweek: int) -> pd.DataFrame:
        """Add SCD Type 2 metadata columns"""
        if df.empty:
            return df
        
        # Create player_id (composite key: player_name + born_year + squad)
        # This handles transfers - same player, different squad = different player_id
        df['player_id'] = df['player_name'] + '_' + df['born_year'].astype(str) + '_' + df['squad']
        
        # Create unique analytics player_key (hash of player_id + gameweek)
        # This creates unique keys for each player-gameweek combination
        def generate_analytics_player_key(row):
            key_string = f"{row['player_id']}_{gameweek}"
            return int(hashlib.md5(key_string.encode()).hexdigest()[:8], 16)
        
        df['player_key'] = df.apply(generate_analytics_player_key, axis=1)
        
        # Add SCD Type 2 metadata
        df['season'] = '2025-2026'  # TODO: Make this dynamic
        df['gameweek'] = gameweek
        df['valid_from'] = datetime.now().date()
        df['valid_to'] = None
        df['is_current'] = True
        
        return df
    
    def get_consolidation_summary(self, outfield_df: pd.DataFrame, goalkeepers_df: pd.DataFrame) -> Dict:
        """Get summary statistics about the consolidated data"""
        return {
            'outfield_players': len(outfield_df),
            'goalkeepers': len(goalkeepers_df),
            'total_players': len(outfield_df) + len(goalkeepers_df),
            'outfield_columns': len(outfield_df.columns) if not outfield_df.empty else 0,
            'goalkeeper_columns': len(goalkeepers_df.columns) if not goalkeepers_df.empty else 0,
            'outfield_teams': outfield_df['squad'].nunique() if not outfield_df.empty else 0,
            'goalkeeper_teams': goalkeepers_df['squad'].nunique() if not goalkeepers_df.empty else 0,
            'total_goals': (
                outfield_df['goals'].sum() if 'goals' in outfield_df.columns else 0
            ) + (
                goalkeepers_df['goals'].sum() if 'goals' in goalkeepers_df.columns else 0
            ),
            'outfield_missing_values': outfield_df.isnull().sum().sum() if not outfield_df.empty else 0,
            'goalkeeper_missing_values': goalkeepers_df.isnull().sum().sum() if not goalkeepers_df.empty else 0,
        }
    
    def validate_consolidation(self, outfield_df: pd.DataFrame, goalkeepers_df: pd.DataFrame) -> Dict:
        """Validate the consolidation results"""
        validation_results = {
            'success': True,
            'errors': [],
            'warnings': []
        }
        
        # Check outfield players
        if outfield_df.empty:
            validation_results['errors'].append("No outfield players consolidated")
            validation_results['success'] = False
        else:
            # Check for key columns
            required_outfield_cols = ['player_key', 'player_name', 'squad', 'position', 'goals', 'assists']
            missing_cols = [col for col in required_outfield_cols if col not in outfield_df.columns]
            if missing_cols:
                validation_results['errors'].append(f"Missing required outfield columns: {missing_cols}")
                validation_results['success'] = False
        
        # Check goalkeepers
        if goalkeepers_df.empty:
            validation_results['warnings'].append("No goalkeepers consolidated")
        else:
            # Check for key columns
            required_keeper_cols = ['player_key', 'player_name', 'squad', 'saves', 'goals_against']
            missing_cols = [col for col in required_keeper_cols if col not in goalkeepers_df.columns]
            if missing_cols:
                validation_results['errors'].append(f"Missing required goalkeeper columns: {missing_cols}")
                validation_results['success'] = False
        
        return validation_results