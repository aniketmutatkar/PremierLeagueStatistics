"""
Enhanced SCD Type 2 Processor - NEW FIXTURE-BASED IMPLEMENTATION
Phase 3: Team-selective historical marking
"""
import pandas as pd
import logging
from datetime import datetime
from typing import List, Dict, Any, Tuple

logger = logging.getLogger(__name__)

from src.scraping.fbref_scraper import FBRefScraper

class SCDType2Processor:
    """NEW: SCD Type 2 processor with team-selective marking"""
    
    def __init__(self, analytics_conn):
        self.conn = analytics_conn
    
    def process_all_updates(self, outfield_df: pd.DataFrame, goalkeepers_df: pd.DataFrame, 
                           gameweek: int, squad_df: pd.DataFrame = None, opponent_df: pd.DataFrame = None) -> bool:
        """
        NEW: Process SCD Type 2 updates with team-selective marking
        
        Args:
            outfield_df: Outfield player data (with gameweek already assigned)
            goalkeepers_df: Goalkeeper data (with gameweek already assigned)
            gameweek: Gameweek being processed
            squad_df: Squad data (with gameweek already assigned)
            opponent_df: Opponent data (with gameweek already assigned)
            
        NOTE: gameweek parameter is NOW the specific gameweek being processed,
              not a global current gameweek
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Processing SCD Type 2 updates for gameweek {gameweek}")
            
            # NEW: Get teams being updated in this gameweek
            teams_in_update = set()
            if not outfield_df.empty:
                teams_in_update.update(outfield_df['squad'].unique())
            if not goalkeepers_df.empty:
                teams_in_update.update(goalkeepers_df['squad'].unique())
            
            logger.info(f"  Teams being updated: {len(teams_in_update)} - {list(teams_in_update)}")
            
            # Process outfield players
            if not outfield_df.empty:
                if not self._process_players_for_teams(outfield_df, gameweek, 'analytics_players', teams_in_update):
                    return False
            
            # Process goalkeepers
            if not goalkeepers_df.empty:
                if not self._process_players_for_teams(goalkeepers_df, gameweek, 'analytics_keepers', teams_in_update):
                    return False
            
            # Process squads
            if squad_df is not None and not squad_df.empty:
                if not self._process_entities_for_teams(squad_df, gameweek, 'analytics_squads', 'squad', teams_in_update):
                    return False
            
            # Process opponents
            if opponent_df is not None and not opponent_df.empty:
                if not self._process_entities_for_teams(opponent_df, gameweek, 'analytics_opponents', 'opponent', teams_in_update):
                    return False
            
            # Validate SCD integrity for this gameweek
            if not self.validate_scd_integrity(gameweek, 'analytics_players'):
                logger.error("SCD integrity validation failed for analytics_players")
                return False
            
            logger.info("✅ SCD Type 2 processing successful")
            return True
            
        except Exception as e:
            logger.error(f"SCD Type 2 processing failed: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def _process_players_for_teams(self, new_data: pd.DataFrame, gameweek: int, table: str, teams: set) -> bool:
        """
        NEW: Process player updates for specific teams only
        
        Args:
            new_data: New player data
            gameweek: Gameweek being processed
            table: Target table
            teams: Set of teams being updated
        """
        try:
            logger.info(f"Processing {len(new_data)} records for {table}")
            
            # NEW: Mark only these teams' current records as historical
            self._mark_current_as_historical_for_teams(table, teams)
            
            # Prepare new records with SCD Type 2 metadata
            # NOTE: gameweek is already in new_data from analytics_etl
            scd_data = self._prepare_scd_records(new_data)
            
            # Insert new current records
            self._insert_new_current_records(scd_data, table)
            
            logger.info(f"✅ {table} processing completed")
            return True
            
        except Exception as e:
            logger.error(f"{table} processing failed: {e}")
            return False
    
    def _process_entities_for_teams(self, new_data: pd.DataFrame, gameweek: int, table: str, entity_type: str, teams: set) -> bool:
        """
        NEW: Process entity updates for specific teams only
        
        Args:
            new_data: New entity data
            gameweek: Gameweek being processed
            table: Target table
            entity_type: 'squad' or 'opponent'
            teams: Set of teams being updated
        """
        try:
            logger.info(f"Processing {len(new_data)} {entity_type}s for {table}")
            
            # NEW: Mark only these teams' current records as historical
            self._mark_current_as_historical_for_teams(table, teams, entity_type)
            
            # Prepare new records
            scd_data = self._prepare_entity_scd_records(new_data, entity_type)
            
            # Insert new current records
            self._insert_new_current_records(scd_data, table)
            
            logger.info(f"✅ {table} processing completed")
            return True
            
        except Exception as e:
            logger.error(f"{table} processing failed: {e}")
            return False
    
    def _mark_current_as_historical_for_teams(self, table: str, teams: set, entity_type: str = 'player') -> None:
        """Mark current records as historical for SPECIFIC teams only"""
        
        # Check if table exists first
        tables = self.conn.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]
        if table not in table_names:
            logger.info(f"Table {table} doesn't exist yet, skipping historical marking")
            return
        
        current_date = datetime.now().date()
        
        # Determine column name based on entity type
        if entity_type in ['squad', 'opponent']:
            team_column = 'squad_name'
        else:
            team_column = 'squad'
        
        # For opponents, add "vs " prefix to match database format
        if entity_type == 'opponent':
            teams_to_query = {'vs ' + team for team in teams}
        else:
            teams_to_query = teams
        
        # Count records before update
        placeholders = ','.join(['?' for _ in teams_to_query])
        count_before = self.conn.execute(f"""
            SELECT COUNT(*) FROM {table} 
            WHERE is_current = true 
            AND {team_column} IN ({placeholders})
        """, list(teams_to_query)).fetchone()[0]
        
        if count_before == 0:
            logger.info(f"No current records to mark for teams in {table}")
            return
        
        # Mark as historical using parameterized query
        self.conn.execute(f"""
            UPDATE {table} 
            SET is_current = false,
                valid_to = ?
            WHERE is_current = true
            AND {team_column} IN ({placeholders})
        """, [current_date] + list(teams_to_query))
        
        logger.info(f"Marked {count_before} records as historical for {len(teams)} teams in {table}")

    def _prepare_scd_records(self, new_data: pd.DataFrame) -> pd.DataFrame:
        """
        NEW: Prepare player records with SCD Type 2 metadata
        
        NOTE: gameweek is already in new_data from analytics_etl.py
        """
        scd_data = new_data.copy()
        
        scraper = FBRefScraper()
        scd_data['season'] = scraper._extract_season_info()
        
        current_date = datetime.now().date()
        
        # Add SCD Type 2 columns (gameweek already present)
        scd_data['valid_from'] = current_date
        scd_data['valid_to'] = None
        scd_data['is_current'] = True
        
        # Generate business keys
        scd_data['player_id'] = scd_data['player_name'] + '_' + scd_data['born_year'].astype(str) + '_' + scd_data['squad'] + '_' + scd_data['season']
        
        return scd_data
    
    def _prepare_entity_scd_records(self, new_data: pd.DataFrame, entity_type: str) -> pd.DataFrame:
        """
        NEW: Prepare entity records with SCD Type 2 metadata
        
        NOTE: gameweek is already in new_data from analytics_etl.py
        """
        scd_data = new_data.copy()
        
        scraper = FBRefScraper()
        scd_data['season'] = scraper._extract_season_info()
        
        current_date = datetime.now().date()
        
        # Add SCD Type 2 columns (gameweek already present)
        scd_data['valid_from'] = current_date
        scd_data['valid_to'] = None
        scd_data['is_current'] = True
        
        # Generate business keys
        if entity_type == 'squad':
            scd_data['squad_id'] = scd_data['squad_name'] + '_' + scd_data['season']
        elif entity_type == 'opponent':
            scd_data['opponent_id'] = scd_data['squad_name'] + '_' + scd_data['season']
        
        return scd_data

    def _insert_new_current_records(self, scd_data: pd.DataFrame, table: str) -> None:
        """Insert new current records into specified analytics table"""
        
        # Ensure valid_to is DATE type, not inferred as INTEGER
        if 'valid_to' in scd_data.columns:
            scd_data['valid_to'] = pd.to_datetime(scd_data['valid_to']).dt.date
        
        # Check if table exists, if not create it
        tables = self.conn.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]
        
        if table not in table_names:
            # Create table from dataframe with explicit DATE type for valid_to
            self.conn.register('temp_scd_data', scd_data)
            self.conn.execute(f"CREATE TABLE {table} AS SELECT * FROM temp_scd_data")
            
            # EXPLICITLY set valid_to to DATE type after creation
            self.conn.execute(f"ALTER TABLE {table} ALTER COLUMN valid_to SET DATA TYPE DATE")
            
            self.conn.unregister('temp_scd_data')
            logger.info(f"Created {table} with {len(scd_data)} records")
            return
        
        # Table exists - insert records normally
        table_columns = self._get_table_columns(table)
        insert_columns = [col for col in scd_data.columns if col in table_columns]
        
        if not insert_columns:
            logger.error(f"No matching columns found for {table}")
            return
        
        insert_data = scd_data[insert_columns]
        
        inserted_count = 0
        for _, row in insert_data.iterrows():
            try:
                columns_str = ', '.join(insert_columns)
                placeholders = ', '.join(['?' for _ in insert_columns])
                values = [row[col] for col in insert_columns]
                
                insert_sql = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
                self.conn.execute(insert_sql, values)
                inserted_count += 1
                
            except Exception as e:
                logger.error(f"Failed to insert record: {e}")
                continue
        
        logger.info(f"Inserted {inserted_count} new current records into {table}")

    def _get_table_columns(self, table: str) -> List[str]:
        """Get list of columns in specified analytics table"""
        try:
            columns_info = self.conn.execute(f"PRAGMA table_info({table})").fetchall()
            return [col[1] for col in columns_info]
        except Exception as e:
            logger.error(f"Failed to get table columns for {table}: {e}")
            return []
    
    def validate_scd_integrity(self, gameweek: int, table: str = 'analytics_players') -> Tuple[bool, List[str]]:
        """Validate SCD Type 2 integrity after updates"""
        issues = []
        
        try:
            if 'player' in table:
                id_column = 'player_id'
                entity_type = 'players'
            elif 'squad' in table:
                id_column = 'squad_id'
                entity_type = 'squads'
            elif 'opponent' in table:
                id_column = 'opponent_id' 
                entity_type = 'opponents'
            else:
                id_column = 'player_id'
                entity_type = 'entities'

            # Check 1: Current records for this gameweek exist
            current_count = self.conn.execute(f"""
                SELECT COUNT(*) 
                FROM {table} 
                WHERE is_current = true AND gameweek = ?
            """, [gameweek]).fetchone()[0]
            
            if current_count == 0:
                issues.append(f"{table}: No current records for gameweek {gameweek}")
            
            # Check 2: No duplicate current records for same entity
            duplicates = self.conn.execute(f"""
                SELECT {id_column}, COUNT(*) as duplicate_count
                FROM {table} 
                WHERE is_current = true
                GROUP BY {id_column}
                HAVING COUNT(*) > 1
            """).fetchall()

            if duplicates:
                issues.append(f"{table}: Found {len(duplicates)} {entity_type} with duplicate current records")
            
            return len(issues) == 0, issues
            
        except Exception as e:
            return False, [f"{table} SCD validation error: {str(e)}"]
    
    def validate_complete_scd_integrity(self, gameweek: int) -> bool:
        """Validate SCD Type 2 integrity for all tables"""
        try:
            all_tables = ['analytics_players', 'analytics_keepers', 'analytics_squads', 'analytics_opponents']
            
            for table in all_tables:
                try:
                    count = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    if count == 0:
                        logger.info(f"Skipping validation for empty table: {table}")
                        continue
                except:
                    logger.info(f"Skipping validation for non-existent table: {table}")
                    continue
                
                is_valid, issues = self.validate_scd_integrity(gameweek, table)
                if not is_valid:
                    logger.error(f"SCD validation failed for {table}: {issues}")
                    return False
            
            logger.info("✅ SCD integrity validation passed for all tables")
            return True
            
        except Exception as e:
            logger.error(f"Complete SCD validation failed: {e}")
            return False
    
    def get_scd_summary(self) -> Dict[str, Any]:
        """Get summary of SCD Type 2 data across all tables"""
        try:
            summary = {}
            
            players_summary = self._get_table_summary('analytics_players')
            keepers_summary = self._get_table_summary('analytics_keepers')
            
            summary['analytics_players'] = players_summary
            summary['analytics_keepers'] = keepers_summary
            
            summary['total_current_records'] = players_summary['current_records'] + keepers_summary['current_records']
            summary['total_historical_records'] = players_summary['historical_records'] + keepers_summary['historical_records']
            summary['total_records'] = players_summary['total_records'] + keepers_summary['total_records']
            
            return summary
            
        except Exception as e:
            logger.error(f"Failed to get SCD summary: {e}")
            return {"error": str(e)}
    
    def _get_table_summary(self, table: str) -> Dict[str, Any]:
        """Get SCD summary for specific table"""
        summary = {}
        
        summary['total_records'] = self.conn.execute(
            f"SELECT COUNT(*) FROM {table}"
        ).fetchone()[0]
        
        summary['current_records'] = self.conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE is_current = true"
        ).fetchone()[0]
        
        summary['historical_records'] = self.conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE is_current = false"
        ).fetchone()[0]
        
        summary['unique_players'] = self.conn.execute(
            f"SELECT COUNT(DISTINCT player_id) FROM {table}"
        ).fetchone()[0]
        
        gameweek_range = self.conn.execute(f"""
            SELECT MIN(gameweek) as min_gw, MAX(gameweek) as max_gw 
            FROM {table}
        """).fetchone()
        
        if gameweek_range and gameweek_range[0] is not None:
            summary['gameweek_range'] = f"{gameweek_range[0]} to {gameweek_range[1]}"
        else:
            summary['gameweek_range'] = "No data"
        
        return summary