"""
Enhanced SCD Type 2 Processor - Handles both players and keepers
"""
import pandas as pd
import logging
from datetime import datetime
from typing import List, Dict, Any, Tuple

logger = logging.getLogger(__name__)

from src.scraping.fbref_scraper import FBRefScraper

class SCDType2Processor:
    """Handles SCD Type 2 updates for analytics tables"""
    
    def __init__(self, analytics_conn):
        self.conn = analytics_conn
    
    def process_player_updates(self, new_data: pd.DataFrame, gameweek: int, table: str = 'analytics_players') -> bool:
        """
        Process SCD Type 2 updates for player data
        
        Args:
            new_data: New player data for current gameweek
            gameweek: Current gameweek number
            table: Target table ('analytics_players' or 'analytics_keepers')
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Processing SCD Type 2 updates for {len(new_data)} records in {table}")
            
            # Step 1: Mark existing current records as historical
            self._mark_current_as_historical(table)
            
            # Step 2: Prepare new records with SCD Type 2 metadata
            scd_data = self._prepare_scd_records(new_data, gameweek)
            
            # Step 3: Insert new current records
            self._insert_new_current_records(scd_data, table)
            
            logger.info(f"✅ SCD Type 2 processing completed successfully for {table}")
            return True
            
        except Exception as e:
            logger.error(f"SCD Type 2 processing failed for {table}: {e}")
            return False
    
    def process_all_updates(self, outfield_df: pd.DataFrame, goalkeepers_df: pd.DataFrame, gameweek: int) -> bool:
        """
        Process SCD Type 2 updates for both outfield players and goalkeepers
        
        Args:
            outfield_df: Outfield player data
            goalkeepers_df: Goalkeeper data  
            gameweek: Current gameweek number
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Processing complete SCD Type 2 updates for gameweek {gameweek}")
            
            # Process outfield players
            if not outfield_df.empty:
                if not self.process_player_updates(outfield_df, gameweek, 'analytics_players'):
                    return False
            
            # Process goalkeepers
            if not goalkeepers_df.empty:
                if not self.process_player_updates(goalkeepers_df, gameweek, 'analytics_keepers'):
                    return False
            
            # Validate overall SCD integrity
            if not self.validate_complete_scd_integrity(gameweek):
                logger.error("SCD integrity validation failed")
                return False
            
            logger.info("✅ Complete SCD Type 2 processing successful")
            return True
            
        except Exception as e:
            logger.error(f"Complete SCD Type 2 processing failed: {e}")
            return False
    
    def _mark_current_as_historical(self, table: str) -> None:
        """Mark all current records as historical for specified table"""
        current_date = datetime.now().date()
        
        # First count current records
        current_count = self.conn.execute(f"""
            SELECT COUNT(*) FROM {table} WHERE is_current = true
        """).fetchone()[0]
        
        # Mark current records as historical
        self.conn.execute(f"""
            UPDATE {table} 
            SET is_current = false,
                valid_to = ?
            WHERE is_current = true
        """, [current_date])
        
        logger.info(f"Marked {current_count} records as historical in {table}")
    
    def _prepare_scd_records(self, new_data: pd.DataFrame, gameweek: int) -> pd.DataFrame:
        """Prepare new records with SCD Type 2 metadata"""
        scd_data = new_data.copy()
        scraper = FBRefScraper()
        
        current_date = datetime.now().date()
        
        # Add SCD Type 2 columns
        scd_data['gameweek'] = gameweek
        scd_data['valid_from'] = current_date
        scd_data['valid_to'] = None
        scd_data['is_current'] = True
        scd_data['season'] = scraper._extract_season_info()
        
        # Generate business keys (we'll let database handle player_key auto-increment)
        scd_data['player_id'] = scd_data['player_name'] + '_' + scd_data['born_year'].astype(str) + '_' + scd_data['squad']
        
        return scd_data
    
    def _insert_new_current_records(self, scd_data: pd.DataFrame, table: str) -> None:
        """Insert new current records into specified analytics table"""
        
        # Get the columns that exist in the target table
        table_columns = self._get_table_columns(table)
        
        # Only keep columns that exist in the target table
        insert_columns = [col for col in scd_data.columns if col in table_columns]
        insert_data = scd_data[insert_columns]
        
        # Build dynamic insert statement using named columns (safer than blind inserts)
        inserted_count = 0
        for _, row in insert_data.iterrows():
            try:
                # Build dynamic insert statement
                columns_str = ', '.join(insert_columns)
                placeholders = ', '.join(['?' for _ in insert_columns])
                values = [row[col] for col in insert_columns]
                
                insert_sql = f"""
                INSERT INTO {table} ({columns_str})
                VALUES ({placeholders})
                """
                
                self.conn.execute(insert_sql, values)
                inserted_count += 1
                
            except Exception as e:
                logger.error(f"Failed to insert record for {row.get('player_name', 'unknown')} into {table}: {e}")
                continue
        
        logger.info(f"Inserted {inserted_count} new current records into {table}")
    
    def _get_table_columns(self, table: str) -> List[str]:
        """Get list of columns in specified analytics table"""
        try:
            columns_info = self.conn.execute(f"PRAGMA table_info({table})").fetchall()
            return [col[1] for col in columns_info]  # Column name is at index 1
        except Exception as e:
            logger.error(f"Failed to get table columns for {table}: {e}")
            return []
    
    def validate_scd_integrity(self, gameweek: int, table: str = 'analytics_players') -> Tuple[bool, List[str]]:
        """Validate SCD Type 2 integrity after updates for specific table"""
        issues = []
        
        try:
            # Check 1: All current records have same gameweek
            current_gameweeks = self.conn.execute(f"""
                SELECT DISTINCT gameweek 
                FROM {table} 
                WHERE is_current = true
            """).fetchall()
            
            if len(current_gameweeks) != 1 or current_gameweeks[0][0] != gameweek:
                issues.append(f"{table}: Multiple gameweeks in current records: {current_gameweeks}")
            
            # Check 2: No overlapping valid periods for same player
            duplicates = self.conn.execute(f"""
                SELECT player_id, COUNT(*) as duplicate_count
                FROM {table} 
                WHERE is_current = true
                GROUP BY player_id
                HAVING COUNT(*) > 1
            """).fetchall()

            if duplicates:
                issues.append(f"{table}: Found {len(duplicates)} players with duplicate current records")
            
            # Check 3: All players have current record
            current_players = self.conn.execute(f"SELECT COUNT(DISTINCT player_id) FROM {table} WHERE is_current = true").fetchone()[0]
            current_records = self.conn.execute(f"SELECT COUNT(*) FROM {table} WHERE is_current = true").fetchone()[0]

            if current_records != current_players:
                issues.append(f"{table}: Current records ({current_records}) don't match current players ({current_players})")
            
            return len(issues) == 0, issues
            
        except Exception as e:
            return False, [f"{table} SCD validation error: {str(e)}"]
    
    def validate_complete_scd_integrity(self, gameweek: int) -> bool:
        """Validate SCD Type 2 integrity for both tables"""
        try:
            # Validate players table
            players_valid, players_issues = self.validate_scd_integrity(gameweek, 'analytics_players')
            
            # Validate keepers table
            keepers_valid, keepers_issues = self.validate_scd_integrity(gameweek, 'analytics_keepers')
            
            all_issues = players_issues + keepers_issues
            
            if all_issues:
                logger.error("SCD integrity issues found:")
                for issue in all_issues:
                    logger.error(f"  - {issue}")
                return False
            
            logger.info("✅ SCD integrity validation passed for both tables")
            return True
            
        except Exception as e:
            logger.error(f"SCD integrity validation failed: {e}")
            return False
    
    def get_scd_summary(self) -> Dict[str, Any]:
        """Get summary of SCD Type 2 data across both tables"""
        try:
            summary = {}
            
            # Players summary
            players_summary = self._get_table_summary('analytics_players')
            keepers_summary = self._get_table_summary('analytics_keepers')
            
            summary['analytics_players'] = players_summary
            summary['analytics_keepers'] = keepers_summary
            
            # Combined totals
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
        
        # Total records
        summary['total_records'] = self.conn.execute(
            f"SELECT COUNT(*) FROM {table}"
        ).fetchone()[0]
        
        # Current records
        summary['current_records'] = self.conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE is_current = true"
        ).fetchone()[0]
        
        # Historical records  
        summary['historical_records'] = self.conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE is_current = false"
        ).fetchone()[0]
        
        # Unique players
        summary['unique_players'] = self.conn.execute(
            f"SELECT COUNT(DISTINCT player_id) FROM {table}"
        ).fetchone()[0]
        
        # Gameweek range
        gameweek_range = self.conn.execute(f"""
            SELECT MIN(gameweek) as min_gw, MAX(gameweek) as max_gw 
            FROM {table}
        """).fetchone()
        
        if gameweek_range and gameweek_range[0] is not None:
            summary['gameweek_range'] = f"{gameweek_range[0]} to {gameweek_range[1]}"
        else:
            summary['gameweek_range'] = "No data"
        
        return summary