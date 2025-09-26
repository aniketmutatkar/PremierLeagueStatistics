#!/usr/bin/env python3
"""
Fixtures Processing - Analytics Component
Handles fixtures data processing for analytics database

Part of the unified analytics system alongside:
- data_consolidation.py (players/squads/opponents)
- scd_processor.py (SCD Type 2 handling)
- column_mappings.py (column mapping)
"""

import pandas as pd
import logging
from typing import Optional
from datetime import datetime

# Import production components
from src.scraping.fbref_scraper import FBRefScraper

logger = logging.getLogger(__name__)

class FixturesProcessor:
    """
    Handles fixtures processing for analytics database
    
    Fixtures don't need SCD Type 2 tracking - they're naturally versioned by gameweek
    Simple approach: rebuild table when new gameweek data is available
    """
    
    def __init__(self):
        logger.info("Fixtures processor initialized")
    
    def process_fixtures(self, raw_conn, analytics_conn, current_gameweek: int, force_refresh: bool = False) -> bool:
        """
        Process fixtures from raw to analytics database
        
        Args:
            raw_conn: Connection to raw database
            analytics_conn: Connection to analytics database
            current_gameweek: Current gameweek being processed
            force_refresh: Force complete rebuild
            
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"🏈 Processing fixtures for gameweek {current_gameweek}")
        
        try:
            # Check if update needed
            if not force_refresh and not self._needs_update(analytics_conn, current_gameweek):
                logger.info("✅ Fixtures already current")
                return True
            
            # Process fixtures
            return self._update_fixtures_table(raw_conn, analytics_conn, current_gameweek)
            
        except Exception as e:
            logger.error(f"❌ Fixtures processing failed: {e}")
            return False
    
    def _needs_update(self, analytics_conn, current_gameweek: int) -> bool:
        """Check if fixtures table needs updating"""
        try:
            # Check if table exists
            tables = analytics_conn.execute("SHOW TABLES").fetchall()
            table_names = [t[0] for t in tables]
            
            if 'analytics_fixtures' not in table_names:
                logger.info("🔄 analytics_fixtures table doesn't exist, creating...")
                return True
            
            # Check if we have current gameweek data
            max_gw = analytics_conn.execute("""
                SELECT COALESCE(MAX(current_through_gameweek), 0) 
                FROM analytics_fixtures
            """).fetchone()[0]
            
            if current_gameweek > max_gw:
                logger.info(f"🔄 Fixtures need update: Current GW{current_gameweek} > Table GW{max_gw}")
                return True
            
            return False
            
        except Exception as e:
            logger.warning(f"Error checking fixtures update need: {e}")
            return True  # If we can't check, assume we need to update
    
    def _update_fixtures_table(self, raw_conn, analytics_conn, current_gameweek: int) -> bool:
        """Update fixtures table with season-aware logic"""
        try:
            # Get current season
            scraper = FBRefScraper()
            current_season = scraper._extract_season_info()
            
            # Get all fixture data from raw database
            raw_fixtures_df = raw_conn.execute("SELECT * FROM raw_fixtures").fetchdf()
            if raw_fixtures_df.empty:
                logger.error("No fixtures data found in raw database")
                return False
            
            # Add season to raw fixtures
            raw_fixtures_df['season'] = current_season
            
            # Create analytics fixtures DataFrame
            analytics_fixtures_df = self._create_analytics_fixtures_dataframe(raw_fixtures_df)
            
            # Check if analytics_fixtures table exists
            table_exists = self._table_exists(analytics_conn, 'analytics_fixtures')
            
            if not table_exists:
                # First time - create table
                analytics_conn.register('temp_fixtures', analytics_fixtures_df)
                analytics_conn.execute("CREATE TABLE analytics_fixtures AS SELECT * FROM temp_fixtures")
                analytics_conn.unregister('temp_fixtures')
                logger.info(f"Created analytics_fixtures table with {len(analytics_fixtures_df)} fixtures")
            else:
                # Table exists - smart update logic
                
                # Delete only current season fixtures (if any exist)
                deleted_count = analytics_conn.execute(f"""
                    DELETE FROM analytics_fixtures 
                    WHERE season = '{current_season}'
                """).rowcount
                
                if deleted_count > 0:
                    logger.info(f"Deleted {deleted_count} existing fixtures for current season {current_season}")
                
                # Insert new current season fixtures
                analytics_conn.register('new_fixtures', analytics_fixtures_df)
                analytics_conn.execute("INSERT INTO analytics_fixtures SELECT * FROM new_fixtures")
                analytics_conn.unregister('new_fixtures')
                
                logger.info(f"Added {len(analytics_fixtures_df)} fixtures for {current_season}")
            
            # Create indexes
            self._create_indexes(analytics_conn)
            
            # Log final state
            season_counts = analytics_conn.execute("""
                SELECT season, COUNT(*) as fixture_count 
                FROM analytics_fixtures 
                GROUP BY season 
                ORDER BY season
            """).fetchall()
            
            logger.info("Final fixtures by season:")
            for season, count in season_counts:
                logger.info(f"  {season}: {count} fixtures")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to update fixtures table: {e}")
            return False

    def _table_exists(self, conn, table_name: str) -> bool:
        """Check if table exists"""
        tables = conn.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]
        return table_name in table_names

    def _create_analytics_fixtures_dataframe(self, raw_fixtures_df: pd.DataFrame) -> pd.DataFrame:
        """Transform raw fixtures into analytics fixtures with derived fields"""
        
        df = raw_fixtures_df.copy()
        
        # Ensure numeric columns
        df['home_score'] = pd.to_numeric(df['home_score'], errors='coerce')
        df['away_score'] = pd.to_numeric(df['away_score'], errors='coerce')
        df['xG'] = pd.to_numeric(df.get('xG', None), errors='coerce')
        df['xG.1'] = pd.to_numeric(df.get('xG.1', None), errors='coerce')
        
        # Match outcome
        df['match_outcome'] = df.apply(self._determine_match_outcome, axis=1)
        
        # Winner
        df['winner'] = df.apply(self._determine_winner, axis=1)
        
        # Points for each team
        df['home_points'] = df.apply(self._calculate_home_points, axis=1)
        df['away_points'] = df.apply(self._calculate_away_points, axis=1)
        
        # Goal differences
        df['home_goal_difference'] = df.apply(
            lambda row: row['home_score'] - row['away_score'] if row['is_completed'] else None, axis=1
        )
        df['away_goal_difference'] = df.apply(
            lambda row: row['away_score'] - row['home_score'] if row['is_completed'] else None, axis=1
        )
        
        # Total goals
        df['total_goals'] = df.apply(
            lambda row: row['home_score'] + row['away_score'] if row['is_completed'] else None, axis=1
        )
        
        # Expected goals fields
        df['home_xg'] = df['xG']
        df['away_xg'] = df['xG.1'] 
        df['home_xg_difference'] = df.apply(
            lambda row: row['xG'] - row['xG.1'] if pd.notna(row['xG']) and pd.notna(row['xG.1']) else None, axis=1
        )
        
        # Goal classification
        df['goal_classification'] = df.apply(self._classify_match_by_goals, axis=1)
        
        # Clean sheets
        df['home_clean_sheet'] = df.apply(
            lambda row: row['away_score'] == 0 if row['is_completed'] else None, axis=1
        )
        df['away_clean_sheet'] = df.apply(
            lambda row: row['home_score'] == 0 if row['is_completed'] else None, axis=1
        )
        
        # Competitive match (based on xG difference, fallback to score)
        df['competitive_match'] = df.apply(self._determine_competitive_match, axis=1)
        
        # Season (simplified approach)
        df['season'] = df['match_date'].apply(self._determine_season)
        
        # Metadata
        df['created_at'] = datetime.now()
        df['updated_at'] = datetime.now()
        
        # Select and order columns for final table
        analytics_columns = [
            # Basic fixture info
            'gameweek', 'match_date', 'match_time', 'day_of_week',
            'home_team', 'away_team', 'venue', 'referee', 'attendance',
            
            # Scores and status
            'home_score', 'away_score', 'is_completed',
            
            # Derived match analysis
            'match_outcome', 'winner', 'home_points', 'away_points',
            'home_goal_difference', 'away_goal_difference', 'total_goals',
            
            # Expected goals
            'home_xg', 'away_xg', 'home_xg_difference',
            
            # Match classification
            'goal_classification', 'competitive_match',
            'home_clean_sheet', 'away_clean_sheet',
            
            # Season and identifiers
            'season', 'fixture_id', 'current_through_gameweek',
            
            # Metadata
            'scraped_date', 'created_at', 'updated_at'
        ]
        
        return df[analytics_columns]
    
    def _determine_match_outcome(self, row) -> str:
        """Determine match outcome"""
        if not row['is_completed']:
            return 'Not Played'
        elif pd.isna(row['home_score']) or pd.isna(row['away_score']):
            return 'Unknown'
        elif row['home_score'] > row['away_score']:
            return 'Home Win'
        elif row['away_score'] > row['home_score']:
            return 'Away Win'
        else:
            return 'Draw'
    
    def _determine_winner(self, row):
        """Determine winner"""
        if not row['is_completed']:
            return None
        elif pd.isna(row['home_score']) or pd.isna(row['away_score']):
            return None
        elif row['home_score'] > row['away_score']:
            return row['home_team']
        elif row['away_score'] > row['home_score']:
            return row['away_team']
        else:
            return 'Draw'
    
    def _calculate_home_points(self, row) -> Optional[int]:
        """Calculate points for home team"""
        if not row['is_completed']:
            return None
        elif pd.isna(row['home_score']) or pd.isna(row['away_score']):
            return None
        elif row['home_score'] > row['away_score']:
            return 3
        elif row['home_score'] == row['away_score']:
            return 1
        else:
            return 0
    
    def _calculate_away_points(self, row) -> Optional[int]:
        """Calculate points for away team"""
        if not row['is_completed']:
            return None
        elif pd.isna(row['home_score']) or pd.isna(row['away_score']):
            return None
        elif row['away_score'] > row['home_score']:
            return 3
        elif row['away_score'] == row['home_score']:
            return 1
        else:
            return 0
    
    def _classify_match_by_goals(self, row) -> str:
        """Classify match by total goals"""
        if not row['is_completed']:
            return 'Not Played'
        elif pd.isna(row['home_score']) or pd.isna(row['away_score']):
            return 'Unknown'
        
        total_goals = row['home_score'] + row['away_score']
        if total_goals == 0:
            return 'Goalless'
        elif total_goals >= 5:
            return 'High Scoring'
        elif total_goals >= 3:
            return 'Medium Scoring'
        else:
            return 'Low Scoring'
    
    def _determine_competitive_match(self, row) -> Optional[bool]:
        """Determine if match was competitive based on xG difference, fallback to score"""
        if not row['is_completed']:
            return None
        
        # Primary: Use xG difference if available
        if pd.notna(row['xG']) and pd.notna(row['xG.1']):
            xg_diff = abs(row['xG'] - row['xG.1'])
            return xg_diff <= 0.8
        
        # Fallback: Use score difference
        elif pd.notna(row['home_score']) and pd.notna(row['away_score']):
            score_diff = abs(row['home_score'] - row['away_score'])
            return score_diff <= 1
        
        return None
    
    def _determine_season(self, match_date) -> str:
        """Determine season from match date"""
        if pd.isna(match_date):
            return 'Unknown'
        
        try:
            year = match_date.year
            month = match_date.month
            
            if month >= 8:  # August or later = start of season
                season_start = year
                season_end = year + 1
            else:  # Before August = end of season
                season_start = year - 1
                season_end = year
            
            return f"{season_start}-{season_end}"
        
        except Exception:
            return 'Unknown'
    
    def _create_indexes(self, analytics_conn):
        """Create performance indexes on fixtures table"""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_fixtures_gameweek ON analytics_fixtures(gameweek)",
            "CREATE INDEX IF NOT EXISTS idx_fixtures_date ON analytics_fixtures(match_date)",
            "CREATE INDEX IF NOT EXISTS idx_fixtures_teams ON analytics_fixtures(home_team, away_team)",
            "CREATE INDEX IF NOT EXISTS idx_fixtures_outcome ON analytics_fixtures(match_outcome)",
            "CREATE INDEX IF NOT EXISTS idx_fixtures_completed ON analytics_fixtures(is_completed)",
            "CREATE INDEX IF NOT EXISTS idx_fixtures_season ON analytics_fixtures(season)"
        ]
        
        for index_sql in indexes:
            try:
                analytics_conn.execute(index_sql)
            except Exception as e:
                logger.warning(f"Index creation warning: {e}")
    
    def validate_fixtures_processing(self, analytics_conn) -> bool:
        """Validate that fixtures processing was successful"""
        try:
            # Check table exists
            tables = analytics_conn.execute("SHOW TABLES").fetchall()
            table_names = [t[0] for t in tables]
            
            if 'analytics_fixtures' not in table_names:
                logger.error("❌ analytics_fixtures table not found")
                return False
            
            # Check basic data quality
            stats = analytics_conn.execute("""
                SELECT 
                    COUNT(*) as total_fixtures,
                    COUNT(CASE WHEN is_completed THEN 1 END) as completed_fixtures,
                    COUNT(CASE WHEN match_outcome IN ('Home Win', 'Away Win', 'Draw') THEN 1 END) as valid_outcomes,
                    COUNT(CASE WHEN competitive_match IS NOT NULL THEN 1 END) as competitive_calculated,
                    MAX(current_through_gameweek) as max_gw
                FROM analytics_fixtures
            """).fetchone()
            
            total, completed, valid_outcomes, competitive, max_gw = stats
            
            if total == 0:
                logger.error("❌ No fixtures found in analytics table")
                return False
            
            if completed > 0 and valid_outcomes != completed:
                logger.error(f"❌ Outcome mismatch: {completed} completed but {valid_outcomes} valid outcomes")
                return False
            
            logger.info(f"✅ Fixtures validation passed: {total} total, {completed} completed, GW{max_gw}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Fixtures validation failed: {e}")
            return False