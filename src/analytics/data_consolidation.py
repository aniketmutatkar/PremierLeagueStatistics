"""
Unified Data Consolidator - Phase 3: Fixture-Based Implementation
Handles players, squads, and opponents WITHOUT gameweek parameter

CRITICAL CHANGES FROM OLD VERSION:
1. All consolidation methods NO LONGER require gameweek parameter
2. NO queries for current_through_gameweek column (deleted in Phase 2)
3. Returns data WITHOUT gameweek column (ETL assigns from fixtures)
4. No SCD metadata added here (moved to SCD processor)
"""

import pandas as pd
import logging
from typing import Dict, List, Optional, Tuple
from .column_mappings import get_entity_mappings, EXCLUDED_COLUMNS

logger = logging.getLogger(__name__)

class DataConsolidator:
    """
    NEW: Unified data consolidation for players, squads, and opponents
    Phase 3: No gameweek filtering, ETL handles gameweek assignment
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
    # PUBLIC METHODS - NEW SIGNATURES (NO GAMEWEEK)
    # =====================================================
    
    def consolidate_players(self, raw_conn) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        NEW: Consolidate player data WITHOUT gameweek parameter
        
        Args:
            raw_conn: Connection to raw database
            
        Returns:
            Tuple of (outfield_df, goalkeepers_df)
        """
        logger.info("Starting player consolidation (NO gameweek filtering)")
        
        # Get base players from player_standard (ALL data, no filtering)
        all_players_df = self._get_base_entity_data(raw_conn, 'player')
        if all_players_df.empty:
            logger.error("No players found in player_standard")
            return pd.DataFrame(), pd.DataFrame()
        
        # Separate players by position
        is_goalkeeper = all_players_df['Pos'].str.contains('GK', na=False)
        outfield_players = all_players_df[~is_goalkeeper].copy()
        goalkeepers = all_players_df[is_goalkeeper].copy()
        
        logger.info(f"Found {len(outfield_players)} outfield players and {len(goalkeepers)} goalkeepers")
        
        # Consolidate outfield players
        outfield_df = self._consolidate_entity_data(
            raw_conn, outfield_players, self.player_outfield_tables, 'player', 'outfield'
        )
        
        # Consolidate goalkeepers
        goalkeepers_df = self._consolidate_entity_data(
            raw_conn, goalkeepers, self.player_goalkeeper_tables, 'player', 'goalkeeper'
        )
        
        logger.info(f"Player consolidation complete: {len(outfield_df)} outfield, {len(goalkeepers_df)} goalkeepers")
        return outfield_df, goalkeepers_df
    
    def consolidate_squads(self, raw_conn) -> pd.DataFrame:
        """
        NEW: Consolidate squad data WITHOUT gameweek parameter
        
        Args:
            raw_conn: Connection to raw database
            
        Returns:
            Consolidated squad DataFrame
        """
        logger.info("Starting squad consolidation (NO gameweek filtering)")
        
        # Get base squad data (ALL data, no filtering)
        base_squads_df = self._get_base_entity_data(raw_conn, 'squad')
        if base_squads_df.empty:
            logger.error("No squads found in squad_standard")
            return pd.DataFrame()
        
        # Consolidate all squad tables
        squad_df = self._consolidate_entity_data(
            raw_conn, base_squads_df, self.entity_tables['squad'], 'squad'
        )
        
        logger.info(f"Squad consolidation complete: {len(squad_df)} squads")
        return squad_df
    
    def consolidate_opponents(self, raw_conn) -> pd.DataFrame:
        """
        NEW: Consolidate opponent data WITHOUT gameweek parameter
        
        Args:
            raw_conn: Connection to raw database
            
        Returns:
            Consolidated opponent DataFrame
        """
        logger.info("Starting opponent consolidation (NO gameweek filtering)")
        
        # Get base opponent data (ALL data, no filtering)
        base_opponents_df = self._get_base_entity_data(raw_conn, 'opponent')
        if base_opponents_df.empty:
            logger.error("No opponents found in opponent_standard")
            return pd.DataFrame()
        
        # Consolidate all opponent tables
        opponent_df = self._consolidate_entity_data(
            raw_conn, base_opponents_df, self.entity_tables['opponent'], 'opponent'
        )
        
        logger.info(f"Opponent consolidation complete: {len(opponent_df)} opponents")
        return opponent_df
    
    # =====================================================
    # CORE PRIVATE METHODS - NEW IMPLEMENTATION
    # =====================================================
    
    def _get_base_entity_data(self, raw_conn, entity_type: str) -> pd.DataFrame:
        """
        NEW: Get base entity data WITHOUT gameweek filtering
        """
        base_table = f"{entity_type}_standard"
        
        try:
            # NEW: No WHERE clause, get ALL data
            query = f"SELECT * FROM {base_table}"
            
            df = pd.read_sql(query, raw_conn)
            
            if not df.empty:
                # Create entity key for joining
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
                                entity_type: str, player_type: Optional[str] = None) -> pd.DataFrame:
        """
        NEW: Core consolidation WITHOUT gameweek parameter
        """
        logger.info(f"Consolidating {len(base_df)} {entity_type}s using {len(tables)} tables")
        
        # Apply mappings to base table
        base_table = tables[0]
        result_df = self._apply_table_mappings(base_df, base_table, entity_type, player_type)
        
        # Process each additional table
        for table_name in tables[1:]:
            try:
                # Get raw data from this table (NO gameweek filtering)
                table_df = self._get_table_data(raw_conn, table_name, entity_type)
                
                if table_df.empty:
                    logger.warning(f"No data found in {table_name}")
                    continue
                
                # Apply mappings
                mapped_df = self._apply_table_mappings(table_df, table_name, entity_type, player_type)
                
                # Merge with main result
                result_df = self._merge_table_data(result_df, mapped_df, table_name, entity_type)
                
                logger.debug(f"Successfully processed {table_name}: {len(mapped_df)} records")
                
            except Exception as e:
                logger.error(f"Error processing {table_name}: {e}")
                continue
        
        # Clean up temporary columns
        result_df = result_df.drop(columns=['entity_key'], errors='ignore')
        
        logger.info(f"{entity_type.title()} consolidation complete: {len(result_df)} entities, {len(result_df.columns)} columns")
        return result_df
    
    def _get_table_data(self, raw_conn, table_name: str, entity_type: str) -> pd.DataFrame:
        """
        NEW: Get data WITHOUT gameweek filtering
        """
        try:
            # NEW: No WHERE clause, get ALL data
            query = f"SELECT * FROM {table_name}"
            
            df = pd.read_sql(query, raw_conn)
            
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
        Apply explicit column mappings (UNCHANGED)
        """
        if df.empty:
            return df
        
        # Get the appropriate mapping dictionary
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
        
        # Build rename dictionary
        rename_dict = {}
        unmapped_columns = []
        missing_columns = []
        
        for raw_col, analytics_col in table_mappings.items():
            if raw_col in df.columns:
                rename_dict[raw_col] = analytics_col
            else:
                missing_columns.append(raw_col)
        
        # Check for unmapped statistical columns
        key_col = self._get_key_column(entity_type)
        for col in df.columns:
            if (col not in table_mappings and 
                col not in EXCLUDED_COLUMNS and 
                col != key_col):
                unmapped_columns.append(col)
        
        # Log any issues
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
    
    def _merge_table_data(self, main_df: pd.DataFrame, new_df: pd.DataFrame, 
                         table_name: str, entity_type: str) -> pd.DataFrame:
        """
        Merge new table data (UNCHANGED)
        """
        try:
            key_col = self._get_key_column(entity_type)
            
            # Perform left join
            merged_df = main_df.merge(new_df, on=key_col, how='left')
            
            # Check for conflicts
            original_cols = set(main_df.columns)
            new_cols = set(new_df.columns) - {key_col}
            conflicts = original_cols & new_cols
            
            if conflicts:
                logger.error(f"Column conflicts detected when merging {table_name}: {conflicts}")
            
            logger.debug(f"Merged {table_name}: added {len(new_cols)} columns")
            return merged_df
            
        except Exception as e:
            logger.error(f"Error merging {table_name}: {e}")
            return main_df
    
    # =====================================================
    # UTILITY METHODS
    # =====================================================
    
    def _create_entity_key(self, df: pd.DataFrame, entity_type: str) -> pd.DataFrame:
        """Create entity key for joining"""
        if entity_type == 'player':
            df['entity_key'] = df['Player'] + '_' + df['Born'].astype(str) + '_' + df['Squad']
        else:  # squad or opponent
            df['entity_key'] = df['Squad']
        return df
    
    def _get_key_column(self, entity_type: str) -> str:
        """Get the temporary key column name"""
        return 'entity_key'
    
    # =====================================================
    # VALIDATION METHODS
    # =====================================================
    
    def get_consolidation_summary(self, **dataframes) -> Dict:
        """Get summary statistics"""
        summary = {}
        
        for entity_name, df in dataframes.items():
            if df is not None and not df.empty:
                summary[f"{entity_name}_count"] = len(df)
                summary[f"{entity_name}_columns"] = len(df.columns)
                summary[f"{entity_name}_missing_values"] = df.isnull().sum().sum()
                
                # Entity-specific stats
                if 'squad_name' in df.columns:
                    summary[f"{entity_name}_unique_squads"] = df['squad_name'].nunique()
                if 'squad' in df.columns:
                    summary[f"{entity_name}_unique_squads"] = df['squad'].nunique()
            else:
                summary[f"{entity_name}_count"] = 0
                summary[f"{entity_name}_columns"] = 0
        
        # Calculate totals
        total_entities = sum(v for k, v in summary.items() if k.endswith('_count'))
        summary['total_entities'] = total_entities
        
        return summary
    
    def validate_consolidation(self, **dataframes) -> Dict:
        """Validate consolidation results"""
        
        validation_results = {
            'success': True,
            'errors': [],
            'warnings': []
        }
        
        for entity_name, df in dataframes.items():
            if df is None or df.empty:
                validation_results['warnings'].append(f"No {entity_name} data consolidated")
                continue
            
            # Check for required columns based on entity type
            if 'player' in entity_name.lower() or 'outfield' in entity_name.lower() or 'goalkeeper' in entity_name.lower():
                required_cols = ['player_name', 'squad']
            else:
                required_cols = ['squad_name']
            
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                validation_results['errors'].append(f"Missing required columns in {entity_name}: {missing_cols}")
                validation_results['success'] = False
        
        return validation_results