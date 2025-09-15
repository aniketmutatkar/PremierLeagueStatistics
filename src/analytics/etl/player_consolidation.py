"""
Player Data Consolidation - Joins all 11 player stat tables into wide format
"""
import pandas as pd
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class PlayerDataConsolidator:
    """Consolidates player data from all 11 raw stat tables"""
    
    def __init__(self):
        # Define all 11 player stat tables
        self.player_tables = [
            'player_standard',      # Base table - all players exist here
            'player_keepers',
            'player_keepersadv', 
            'player_shooting',
            'player_passing',
            'player_passingtypes',
            'player_goalshotcreation',
            'player_defense',
            'player_possession',
            'player_playingtime',
            'player_misc'
        ]
    
    def consolidate_players(self, raw_conn, gameweek: int) -> pd.DataFrame:
        """
        Consolidate all player data from 11 tables into wide format
        
        Args:
            raw_conn: Connection to raw database
            gameweek: Target gameweek to consolidate
            
        Returns:
            DataFrame with consolidated player stats
        """
        logger.info(f"Starting player consolidation for gameweek {gameweek}")
        
        # Step 1: Get base player data from player_standard
        base_df = self._get_base_player_data(raw_conn, gameweek)
        if base_df.empty:
            logger.error("No base player data found")
            return pd.DataFrame()
        
        logger.info(f"Base data loaded: {len(base_df)} players")
        
        # Step 2: Join each additional stat table
        consolidated_df = base_df.copy()
        
        for table in self.player_tables[1:]:  # Skip player_standard (already loaded)
            try:
                stat_df = self._get_table_data(raw_conn, table, gameweek)
                if not stat_df.empty:
                    consolidated_df = self._join_stat_table(consolidated_df, stat_df, table)
                    logger.debug(f"Joined {table}: {len(stat_df)} records")
                else:
                    logger.warning(f"No data found in {table} for gameweek {gameweek}")
                    
            except Exception as e:
                logger.error(f"Error processing {table}: {e}")
                # Continue with other tables even if one fails
                continue
        
        # Step 3: Clean and standardize column names
        consolidated_df = self._standardize_columns(consolidated_df)
        
        logger.info(f"Consolidation complete: {len(consolidated_df)} players, {len(consolidated_df.columns)} columns")
        return consolidated_df
    
    def _get_base_player_data(self, raw_conn, gameweek: int) -> pd.DataFrame:
        """Get base player data from player_standard table"""
        query = """
        SELECT 
            Player,
            Born,
            Nation,
            Pos as position,
            Squad,
            Age,
            "Playing Time MP" as matches_played,
            "Playing Time Starts" as starts, 
            "Playing Time Min" as minutes_played,
            "Playing Time 90s" as minutes_90s,
            "Performance Gls" as goals,
            "Performance Ast" as assists,
            "Performance G+A" as goals_plus_assists,
            "Performance G-PK" as non_penalty_goals,
            "Performance PK" as penalty_kicks_made,
            "Performance PKatt" as penalty_kicks_attempted,
            "Performance CrdY" as yellow_cards,
            "Performance CrdR" as red_cards,
            "Expected xG" as expected_goals,
            "Expected npxG" as non_penalty_expected_goals,
            "Expected xAG" as expected_assisted_goals,
            "Expected npxG+xAG" as expected_goals_plus_assists,
            "Progression PrgC" as progressive_carries,
            "Progression PrgP" as progressive_passes,
            current_through_gameweek
        FROM player_standard 
        WHERE current_through_gameweek = ?
        """
        
        try:
            # Suppress pandas warning
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                df = pd.read_sql(query, raw_conn, params=[gameweek])
            
            # Create composite key for joining
            df['player_key'] = df['Player'] + '_' + df['Born'].astype(str)
            
            # CRITICAL: Remove duplicates if they exist
            initial_count = len(df)
            df = df.drop_duplicates(subset=['player_key'], keep='first')
            final_count = len(df)
            
            if initial_count != final_count:
                logger.warning(f"Removed {initial_count - final_count} duplicate players from base data")
            
            return df
        except Exception as e:
            logger.error(f"Error loading base player data: {e}")
            return pd.DataFrame()
    
    def _get_table_data(self, raw_conn, table_name: str, gameweek: int) -> pd.DataFrame:
        """Get data from a specific player stat table"""
        try:
            # Get all columns for this table
            columns_query = f"PRAGMA table_info({table_name})"
            
            # Suppress pandas warning
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                columns_info = pd.read_sql(columns_query, raw_conn)
            
            columns = columns_info['name'].tolist()
            
            # Build query - always include Player, Born for joining
            if 'Player' not in columns or 'Born' not in columns:
                logger.warning(f"Table {table_name} missing Player or Born columns")
                return pd.DataFrame()
            
            # Select all columns from the table
            query = f"""
            SELECT * FROM {table_name} 
            WHERE current_through_gameweek = ?
            """
            
            # Suppress pandas warning
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                df = pd.read_sql(query, raw_conn, params=[gameweek])
            
            if not df.empty:
                # Create composite key for joining
                df['player_key'] = df['Player'] + '_' + df['Born'].astype(str)
                
                # CRITICAL: Remove duplicates by player_key, keep first occurrence
                initial_count = len(df)
                df = df.drop_duplicates(subset=['player_key'], keep='first')
                final_count = len(df)
                
                if initial_count != final_count:
                    logger.warning(f"Removed {initial_count - final_count} duplicates from {table_name}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading data from {table_name}: {e}")
            return pd.DataFrame()
    
    def _join_stat_table(self, base_df: pd.DataFrame, stat_df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Join a stat table to the base dataframe"""
        try:
            # Identify columns to join (exclude common ones)
            exclude_cols = ['Player', 'Born', 'current_through_gameweek', 'last_updated', 'Current Date', 'player_key']
            stat_cols = [col for col in stat_df.columns if col not in exclude_cols]
            
            # Select only the player_key and stat columns
            join_df = stat_df[['player_key'] + stat_cols].copy()
            
            # Add table prefix to avoid column name conflicts
            table_prefix = table_name.replace('player_', '')
            rename_dict = {col: f"{table_prefix}_{col}" for col in stat_cols}
            join_df = join_df.rename(columns=rename_dict)
            
            # Perform left join
            merged_df = base_df.merge(join_df, on='player_key', how='left')
            
            logger.debug(f"Joined {table_name}: added {len(stat_cols)} columns")
            return merged_df
            
        except Exception as e:
            logger.error(f"Error joining {table_name}: {e}")
            return base_df  # Return original if join fails
    
    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names and data types"""
        try:
            # Fill NaN values appropriately
            numeric_columns = df.select_dtypes(include=['number']).columns
            df[numeric_columns] = df[numeric_columns].fillna(0)
            
            # Clean player name (remove any extra whitespace)
            if 'Player' in df.columns:
                df['Player'] = df['Player'].str.strip()
                df = df.rename(columns={'Player': 'player_name'})
            
            if 'Squad' in df.columns:
                df['Squad'] = df['Squad'].str.strip()
                df = df.rename(columns={'Squad': 'squad'})
            
            # Ensure required columns exist
            required_cols = ['player_name', 'squad', 'Born', 'player_key']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                logger.error(f"Missing required columns: {missing_cols}")
            
            logger.info(f"Column standardization complete: {len(df.columns)} total columns")
            return df
            
        except Exception as e:
            logger.error(f"Error standardizing columns: {e}")
            return df
    
    def get_consolidation_summary(self, df: pd.DataFrame) -> dict:
        """Get summary statistics about the consolidated data"""
        if df.empty:
            return {"error": "No data to summarize"}
        
        return {
            "total_players": len(df),
            "total_columns": len(df.columns), 
            "teams": df['squad'].nunique() if 'squad' in df.columns else 0,
            "positions": df['position'].nunique() if 'position' in df.columns else 0,
            "total_goals": df['goals'].sum() if 'goals' in df.columns else 0,
            "avg_minutes": df['minutes_played'].mean() if 'minutes_played' in df.columns else 0,
            "missing_values": df.isnull().sum().sum()
        }