"""
Analytics Database Operations - Core operations for analytics database
"""
import pandas as pd
import logging
from datetime import datetime
from typing import List, Optional, Tuple
from .connection import AnalyticsDBConnection
from src.scraping.fbref_scraper import FBRefScraper

logger = logging.getLogger(__name__)

class AnalyticsDBOperations:
    """Core operations for analytics database"""
    
    def __init__(self):
        self.db = AnalyticsDBConnection()
    
    def get_current_analytics_gameweek(self) -> Optional[int]:
        """Get the latest gameweek in analytics database"""
        try:
            with self.db.get_analytics_connection() as conn:
                result = conn.execute("""
                    SELECT MAX(gameweek) 
                    FROM analytics_players 
                    WHERE is_current = true
                """).fetchone()
                return int(result[0]) if result[0] else None
        except Exception as e:
            logger.error(f"Error getting analytics gameweek: {e}")
            return None
    
    def mark_records_as_historical(self, gameweek: int) -> bool:
        """Mark all current records as historical for SCD Type 2"""
        try:
            with self.db.get_analytics_connection() as conn:
                current_date = datetime.now().date()
                
                # First count current records
                current_count = conn.execute("""
                    SELECT COUNT(*) FROM analytics_players WHERE is_current = true
                """).fetchone()[0]
                
                # Mark current records as historical
                conn.execute("""
                    UPDATE analytics_players 
                    SET is_current = false,
                        valid_to = ?
                    WHERE is_current = true
                """, [current_date])
                
                logger.info(f"Marked {current_count} records as historical for gameweek {gameweek}")
                return True
                
        except Exception as e:
            logger.error(f"Error marking records as historical: {e}")
            return False
    
    def insert_analytics_players(self, player_data: pd.DataFrame, gameweek: int) -> bool:
        """Insert new player records into analytics_players"""
        try:
            with self.db.get_analytics_connection() as conn:
                current_date = datetime.now().date()
                scraper = FBRefScraper()
                
                # Add SCD Type 2 columns
                player_data['gameweek'] = gameweek
                player_data['valid_from'] = current_date
                player_data['valid_to'] = None
                player_data['is_current'] = True
                player_data['season'] = scraper._extract_season_info()
                
                # Insert data
                conn.execute("INSERT INTO analytics_players SELECT * FROM player_data")
                
                rows_inserted = len(player_data)
                logger.info(f"Inserted {rows_inserted} player records for gameweek {gameweek}")
                return True
                
        except Exception as e:
            logger.error(f"Error inserting analytics players: {e}")
            return False
    
    def get_analytics_player_count(self, gameweek: Optional[int] = None) -> int:
        """Get count of players in analytics for specific gameweek"""
        try:
            with self.db.get_analytics_connection() as conn:
                if gameweek:
                    query = """
                        SELECT COUNT(*) 
                        FROM analytics_players 
                        WHERE gameweek = ? AND is_current = true
                    """
                    result = conn.execute(query, [gameweek]).fetchone()
                else:
                    query = "SELECT COUNT(*) FROM analytics_players WHERE is_current = true"
                    result = conn.execute(query).fetchone()
                
                return result[0] if result else 0
                
        except Exception as e:
            logger.error(f"Error getting player count: {e}")
            return 0
    
    def validate_analytics_data_quality(self, gameweek: int) -> Tuple[bool, List[str]]:
        """Validate data quality for analytics layer"""
        issues = []
        
        try:
            with self.db.get_analytics_connection() as conn:
                # Check 1: Minimum player count
                player_count = conn.execute("""
                    SELECT COUNT(*) FROM analytics_players 
                    WHERE gameweek = ? AND is_current = true
                """, [gameweek]).fetchone()[0]
                
                if player_count < 300:  # Expect ~300+ players
                    issues.append(f"Low player count: {player_count} (expected 300+)")
                
                # Check 2: Required teams count  
                team_count = conn.execute("""
                    SELECT COUNT(DISTINCT squad) FROM analytics_players 
                    WHERE gameweek = ? AND is_current = true
                """, [gameweek]).fetchone()[0]
                
                if team_count < 20:  # Premier League has 20 teams
                    issues.append(f"Missing teams: {team_count}/20 teams found")
                
                # Check 3: Null percentage in key fields
                null_check = conn.execute("""
                    SELECT 
                        COALESCE(SUM(CASE WHEN player_name IS NULL THEN 1 ELSE 0 END), 0) as null_names,
                        COALESCE(SUM(CASE WHEN squad IS NULL THEN 1 ELSE 0 END), 0) as null_squads,
                        COUNT(*) as total
                    FROM analytics_players 
                    WHERE gameweek = ? AND is_current = true
                """, [gameweek]).fetchone()
                
                if null_check[2] > 0:  # Only calculate if we have records
                    null_percentage = (null_check[0] + null_check[1]) / null_check[2] * 100
                    if null_percentage > 5:  # Max 5% nulls allowed
                        issues.append(f"High null percentage: {null_percentage:.1f}%")
                else:
                    issues.append("No records found for validation")
                
                logger.info(f"Data quality check for GW{gameweek}: {len(issues)} issues found")
                return len(issues) == 0, issues
                
        except Exception as e:
            logger.error(f"Error validating data quality: {e}")
            return False, [f"Validation error: {str(e)}"]
    
    def get_team_totals(self, raw_conn, gameweek: int) -> pd.DataFrame:
        """Calculate team totals from raw data for derived metrics"""
        try:
            query = """
            SELECT 
                Squad as team_name,
                SUM("Performance Gls") as team_total_goals,
                SUM("Performance Ast") as team_total_assists,
                SUM("Playing Time Min") as team_total_minutes
            FROM player_standard 
            WHERE current_through_gameweek = ?
            GROUP BY Squad
            """
            
            # Suppress pandas warning
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                team_totals = pd.read_sql(query, raw_conn, params=[gameweek])
            
            logger.info(f"Calculated team totals for {len(team_totals)} teams")
            return team_totals
            
        except Exception as e:
            logger.error(f"Error calculating team totals: {e}")
            return pd.DataFrame()