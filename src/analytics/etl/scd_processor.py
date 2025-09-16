"""
SCD Type 2 Processor - Handles slowly changing dimension updates for analytics
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
    
    def process_player_updates(self, new_data: pd.DataFrame, gameweek: int) -> bool:
        """
        Process SCD Type 2 updates for player data
        
        Args:
            new_data: New player data for current gameweek
            gameweek: Current gameweek number
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Processing SCD Type 2 updates for {len(new_data)} players")
            
            # Step 1: Mark existing current records as historical
            self._mark_current_as_historical()
            
            # Step 2: Prepare new records with SCD Type 2 metadata
            scd_data = self._prepare_scd_records(new_data, gameweek)
            
            # Step 3: Insert new current records
            self._insert_new_current_records(scd_data)
            
            logger.info("✅ SCD Type 2 processing completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"SCD Type 2 processing failed: {e}")
            return False
    
    def _mark_current_as_historical(self) -> None:
        """Mark all current records as historical"""
        current_date = datetime.now().date()
        
        # First count current records
        current_count = self.conn.execute("""
            SELECT COUNT(*) FROM analytics_players WHERE is_current = true
        """).fetchone()[0]
        
        # Mark current records as historical
        self.conn.execute("""
            UPDATE analytics_players 
            SET is_current = false,
                valid_to = ?
            WHERE is_current = true
        """, [current_date])
        
        logger.info(f"Marked {current_count} records as historical")
    
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
        scd_data['player_id'] = scd_data['player_name'] + '_' + scd_data['born_year'].astype(str)
        
        return scd_data
    
    def _insert_new_current_records(self, scd_data: pd.DataFrame) -> None:
        """Insert new current records into analytics_players"""
        
        # Get the columns that exist in analytics_players table
        table_columns = self._get_analytics_table_columns()
        
        # Only keep columns that exist in the target table
        insert_columns = [col for col in scd_data.columns if col in table_columns]
        insert_data = scd_data[insert_columns]
        
        # Bulk insert approach using DuckDB's efficient methods
        # For now, we'll use a simple row-by-row approach
        # TODO: Optimize with bulk insert methods
        
        inserted_count = 0
        for _, row in insert_data.iterrows():
            try:
                # Build dynamic insert statement
                columns_str = ', '.join(insert_columns)
                placeholders = ', '.join(['?' for _ in insert_columns])
                values = [row[col] for col in insert_columns]
                
                insert_sql = f"""
                INSERT INTO analytics_players ({columns_str})
                VALUES ({placeholders})
                """
                
                self.conn.execute(insert_sql, values)
                inserted_count += 1
                
            except Exception as e:
                logger.error(f"Failed to insert record for {row.get('player_name', 'unknown')}: {e}")
                continue
        
        logger.info(f"Inserted {inserted_count} new current records")
    
    def _get_analytics_table_columns(self) -> List[str]:
        """Get list of columns in analytics_players table"""
        try:
            columns_info = self.conn.execute("PRAGMA table_info(analytics_players)").fetchall()
            return [col[1] for col in columns_info]  # Column name is at index 1
        except Exception as e:
            logger.error(f"Failed to get table columns: {e}")
            return []
    
    def validate_scd_integrity(self, gameweek: int) -> Tuple[bool, List[str]]:
        """Validate SCD Type 2 integrity after updates"""
        issues = []
        
        try:
            # Check 1: All current records have same gameweek
            current_gameweeks = self.conn.execute("""
                SELECT DISTINCT gameweek 
                FROM analytics_players 
                WHERE is_current = true
            """).fetchall()
            
            if len(current_gameweeks) != 1 or current_gameweeks[0][0] != gameweek:
                issues.append(f"Multiple gameweeks in current records: {current_gameweeks}")
            
            # Check 2: No overlapping valid periods for same player
            overlapping = self.conn.execute("""
                SELECT player_id, COUNT(*) as overlap_count
                FROM analytics_players 
                WHERE valid_to IS NULL OR valid_to >= valid_from
                GROUP BY player_id
                HAVING COUNT(*) > 1
            """).fetchall()
            
            if overlapping:
                issues.append(f"Found {len(overlapping)} players with overlapping valid periods")
            
            # Check 3: All players have current record
            total_current = self.conn.execute("""
                SELECT COUNT(*) FROM analytics_players WHERE is_current = true
            """).fetchone()[0]
            
            expected_players = self.conn.execute("""
                SELECT COUNT(DISTINCT player_id) FROM analytics_players
            """).fetchone()[0]
            
            if total_current != expected_players:
                issues.append(f"Current records ({total_current}) don't match unique players ({expected_players})")
            
            return len(issues) == 0, issues
            
        except Exception as e:
            return False, [f"SCD validation error: {str(e)}"]
    
    def get_scd_summary(self) -> Dict[str, Any]:
        """Get summary of SCD Type 2 data"""
        try:
            summary = {}
            
            # Total records
            summary['total_records'] = self.conn.execute(
                "SELECT COUNT(*) FROM analytics_players"
            ).fetchone()[0]
            
            # Current records
            summary['current_records'] = self.conn.execute(
                "SELECT COUNT(*) FROM analytics_players WHERE is_current = true"
            ).fetchone()[0]
            
            # Historical records  
            summary['historical_records'] = self.conn.execute(
                "SELECT COUNT(*) FROM analytics_players WHERE is_current = false"
            ).fetchone()[0]
            
            # Unique players
            summary['unique_players'] = self.conn.execute(
                "SELECT COUNT(DISTINCT player_id) FROM analytics_players"
            ).fetchone()[0]
            
            # Gameweek range
            gameweek_range = self.conn.execute("""
                SELECT MIN(gameweek) as min_gw, MAX(gameweek) as max_gw 
                FROM analytics_players
            """).fetchone()
            
            summary['gameweek_range'] = f"{gameweek_range[0]} to {gameweek_range[1]}"
            
            return summary
            
        except Exception as e:
            logger.error(f"Failed to get SCD summary: {e}")
            return {"error": str(e)}