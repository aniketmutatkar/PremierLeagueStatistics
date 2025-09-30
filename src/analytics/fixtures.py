#!/usr/bin/env python3
"""
Fixtures Processing - Phase 3: Fixture-Based Implementation
Analytics Component for fixtures data

CRITICAL CHANGE: Remove ONLY current_through_gameweek column reference
"""

import pandas as pd
import logging
from typing import Optional
from datetime import datetime

from src.scraping.fbref_scraper import FBRefScraper

logger = logging.getLogger(__name__)

class FixturesProcessor:
    """Handles fixtures processing for analytics database"""
    
    def __init__(self):
        logger.info("Fixtures processor initialized")
    
    def process_fixtures(self, raw_conn, analytics_conn, current_gameweek: int, force_refresh: bool = False) -> bool:
        """Process fixtures from raw to analytics database"""
        logger.info(f"🏈 Processing fixtures for gameweek {current_gameweek}")
        
        try:
            if not force_refresh and not self._needs_update(analytics_conn, current_gameweek):
                logger.info("✅ Fixtures already current")
                return True
            
            return self._update_fixtures_table(raw_conn, analytics_conn, current_gameweek)
            
        except Exception as e:
            logger.error(f"❌ Fixtures processing failed: {e}")
            return False
    
    def _needs_update(self, analytics_conn, current_gameweek: int) -> bool:
        """Check if fixtures table needs updating"""
        try:
            tables = analytics_conn.execute("SHOW TABLES").fetchall()
            table_names = [t[0] for t in tables]
            
            if 'analytics_fixtures' not in table_names:
                logger.info("🔄 analytics_fixtures table doesn't exist, creating...")
                return True
            
            # CHANGED: Check max gameweek from gameweek column (not current_through_gameweek)
            max_gw = analytics_conn.execute("""
                SELECT COALESCE(MAX(gameweek), 0) 
                FROM analytics_fixtures
            """).fetchone()[0]
            
            if current_gameweek > max_gw:
                logger.info(f"🔄 Fixtures need update: Current GW{current_gameweek} > Table GW{max_gw}")
                return True
            
            return False
            
        except Exception as e:
            logger.warning(f"Error checking fixtures update need: {e}")
            return True
    
    def _update_fixtures_table(self, raw_conn, analytics_conn, current_gameweek: int) -> bool:
        """Update fixtures table with season-aware logic"""
        try:
            scraper = FBRefScraper()
            current_season = scraper._extract_season_info()
            
            raw_fixtures_df = raw_conn.execute("SELECT * FROM raw_fixtures").fetchdf()
            if raw_fixtures_df.empty:
                logger.error("No fixtures data found in raw database")
                return False
            
            raw_fixtures_df['season'] = current_season
            
            analytics_fixtures_df = self._create_analytics_fixtures_dataframe(raw_fixtures_df)
            
            table_exists = self._table_exists(analytics_conn, 'analytics_fixtures')
            
            if not table_exists:
                analytics_conn.register('temp_fixtures', analytics_fixtures_df)
                analytics_conn.execute("CREATE TABLE analytics_fixtures AS SELECT * FROM temp_fixtures")
                analytics_conn.unregister('temp_fixtures')
                logger.info(f"Created analytics_fixtures table with {len(analytics_fixtures_df)} fixtures")
            else:
                analytics_conn.execute("""
                    DELETE FROM analytics_fixtures 
                    WHERE season = ?
                """, [current_season])
                
                analytics_conn.register('temp_fixtures', analytics_fixtures_df)
                analytics_conn.execute("""
                    INSERT INTO analytics_fixtures 
                    SELECT * FROM temp_fixtures
                """)
                analytics_conn.unregister('temp_fixtures')
                
                logger.info(f"Updated analytics_fixtures with {len(analytics_fixtures_df)} fixtures")
            
            return True
            
        except Exception as e:
            logger.error(f"Error updating fixtures table: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def _create_analytics_fixtures_dataframe(self, raw_fixtures_df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform raw fixtures into analytics fixtures with derived fields
        ONLY CHANGE: Don't include current_through_gameweek column
        """
        df = raw_fixtures_df.copy()
        
        # Ensure numeric columns
        df['home_score'] = pd.to_numeric(df['home_score'], errors='coerce')
        df['away_score'] = pd.to_numeric(df['away_score'], errors='coerce')
        df['xG'] = pd.to_numeric(df.get('xG', None), errors='coerce')
        df['xG.1'] = pd.to_numeric(df.get('xG.1', None), errors='coerce')
        
        # Match outcome
        df['match_outcome'] = df.apply(lambda row: 
            'Not Played' if not row['is_completed']
            else 'Home Win' if row['home_score'] > row['away_score']
            else 'Away Win' if row['away_score'] > row['home_score']
            else 'Draw', axis=1)
        
        # Winner
        df['winner'] = df.apply(lambda row:
            None if not row['is_completed']
            else row['home_team'] if row['home_score'] > row['away_score']
            else row['away_team'] if row['away_score'] > row['home_score']
            else 'Draw', axis=1)
        
        # Points
        df['home_points'] = df.apply(lambda row:
            None if not row['is_completed']
            else 3 if row['home_score'] > row['away_score']
            else 1 if row['home_score'] == row['away_score']
            else 0, axis=1)
        
        df['away_points'] = df.apply(lambda row:
            None if not row['is_completed']
            else 3 if row['away_score'] > row['home_score']
            else 1 if row['home_score'] == row['away_score']
            else 0, axis=1)
        
        # Total goals
        df['total_goals'] = df.apply(lambda row:
            None if not row['is_completed']
            else row['home_score'] + row['away_score'], axis=1)
        
        # Goal classification
        df['goal_classification'] = df.apply(lambda row:
            None if not row['is_completed']
            else 'High Scoring' if row['home_score'] + row['away_score'] > 3
            else 'Medium Scoring' if row['home_score'] + row['away_score'] >= 2
            else 'Low Scoring', axis=1)
        
        # Clean sheets
        df['home_clean_sheet'] = df.apply(lambda row:
            None if not row['is_completed']
            else row['away_score'] == 0, axis=1)
        
        df['away_clean_sheet'] = df.apply(lambda row:
            None if not row['is_completed']
            else row['home_score'] == 0, axis=1)
        
        # Competitive match
        df['competitive_match'] = df.apply(lambda row:
            None if not row['is_completed']
            else (abs(row['xG'] - row['xG.1']) <= 0.8) if pd.notna(row['xG']) and pd.notna(row['xG.1'])
            else abs(row['home_score'] - row['away_score']) <= 1, axis=1)
        
        # Metadata
        df['created_at'] = datetime.now()
        
        # ONLY CHANGE: Drop current_through_gameweek if it exists
        df = df.drop(columns=['current_through_gameweek'], errors='ignore')
        
        return df
    
    def _table_exists(self, conn, table_name: str) -> bool:
        """Check if table exists"""
        try:
            tables = conn.execute("SHOW TABLES").fetchall()
            table_names = [t[0] for t in tables]
            return table_name in table_names
        except Exception:
            return False