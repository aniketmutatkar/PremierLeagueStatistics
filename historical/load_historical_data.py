#!/usr/bin/env python3
"""
Historical Data Loader - Uses Production Architecture
One-time loader for historical seasons using existing production pipeline

Architecture:
1. Load historical data into premierleague_raw_historical.duckdb (truncate & reload)
2. Run existing AnalyticsETL against historical raw database  
3. Post-process to mark records as historical (is_current=false)

No modifications to production code needed.
"""

import logging
import sys
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

# Import production components
from src.scraping.fbref_scraper import FBRefScraper
from src.database.raw_db import RawDatabaseConnection, RawDatabaseOperations, initialize_raw_database
from src.analytics.analytics_etl import AnalyticsETL
from src.database.analytics_db import AnalyticsDBConnection

logger = logging.getLogger(__name__)


class HistoricalRawLoader:
    """Loads historical seasons into raw historical database using production logic"""
    
    def __init__(self, season: str = None):
        self.scraper = FBRefScraper(override_season=season)
        
        # Historical raw database (separate from production)
        self.historical_raw_db_path = "data/historical/premierleague_raw_historical.duckdb"
        
        # Historical URL patterns (same as your sources.yaml structure)
        self.historical_url_patterns = {
            'standard': 'https://fbref.com/en/comps/9/{season}/stats/{season}-Premier-League-Stats',
            'shooting': 'https://fbref.com/en/comps/9/{season}/shooting/{season}-Premier-League-Stats',
            'passing': 'https://fbref.com/en/comps/9/{season}/passing/{season}-Premier-League-Stats',
            'passingtypes': 'https://fbref.com/en/comps/9/{season}/passing_types/{season}-Premier-League-Stats',
            'goalshotcreation': 'https://fbref.com/en/comps/9/{season}/gca/{season}-Premier-League-Stats',
            'defense': 'https://fbref.com/en/comps/9/{season}/defense/{season}-Premier-League-Stats',
            'possession': 'https://fbref.com/en/comps/9/{season}/possession/{season}-Premier-League-Stats',
            'playingtime': 'https://fbref.com/en/comps/9/{season}/playingtime/{season}-Premier-League-Stats',
            'misc': 'https://fbref.com/en/comps/9/{season}/misc/{season}-Premier-League-Stats',
            'keepers': 'https://fbref.com/en/comps/9/{season}/keepers/{season}-Premier-League-Stats',
            'keepersadv': 'https://fbref.com/en/comps/9/{season}/keepersadv/{season}-Premier-League-Stats'
        }
        
        logger.info("Historical raw loader initialized")
    
    def truncate_and_load_season(self, season: str) -> bool:
        """
        Truncate historical raw database and load specified season
        
        Args:
            season: Season in format "2024-2025"
            
        Returns:
            True if successful
        """
        logger.info(f"Starting truncate and load for season: {season}")
        
        try:
            # Step 1: Initialize/truncate historical raw database
            if not self._initialize_historical_raw_database():
                return False
            
            # Step 2: Load fixtures for the season
            if not self._load_historical_fixtures(season):
                return False
            
            # Step 3: Load all stat categories for the season
            if not self._load_historical_stats(season):
                return False
            
            logger.info(f"Successfully loaded {season} into historical raw database")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load season {season}: {e}")
            return False
    
    def _initialize_historical_raw_database(self) -> bool:
        """Initialize and truncate historical raw database"""
        
        try:
            logger.info("Initializing historical raw database...")
            
            # Use your existing initialize_raw_database function but with custom path
            # We'll need to create connection manually since function uses config
            from src.database.raw_db.connection import RawDatabaseConnection
            from src.database.raw_db.operations import RawDatabaseOperations
            
            # Create historical raw database connection with custom path
            raw_conn = RawDatabaseConnection()
            # Override the path for historical database
            raw_conn.db_path = self.historical_raw_db_path
            
            raw_ops = RawDatabaseOperations(raw_conn)
            
            # Create infrastructure tables
            raw_ops.create_infrastructure_tables()
            
            # Truncate all tables (fresh start)
            with raw_conn.get_connection() as conn:
                # Get all table names
                tables = conn.execute("SHOW TABLES").fetchall()
                table_names = [table[0] for table in tables]
                
                # Truncate each table
                for table_name in table_names:
                    conn.execute(f"DELETE FROM {table_name}")
                    logger.info(f"Truncated {table_name}")
            
            logger.info("Historical raw database initialized and truncated")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize historical raw database: {e}")
            return False
    
    def _load_historical_fixtures(self, season: str) -> bool:
        """Load historical fixtures using production fixture logic"""
        
        try:
            logger.info(f"Loading historical fixtures for {season}...")
            
            # Use production scraper to get fixtures
            fixtures_url = f"https://fbref.com/en/comps/9/{season}/schedule/{season}-Premier-League-Scores-and-Fixtures"
            fixture_data = self.scraper.scrape_fixtures(fixtures_url)
            
            if not fixture_data:
                logger.error("Failed to scrape historical fixtures")
                return False
            
            # Insert fixtures using production raw database operations
            from src.database.raw_db.connection import RawDatabaseConnection
            from src.database.raw_db.operations import RawDatabaseOperations
            
            # Create connection with custom path
            raw_conn = RawDatabaseConnection()
            raw_conn.db_path = self.historical_raw_db_path
            raw_ops = RawDatabaseOperations(raw_conn)
            
            # Insert fixtures (gameweek 38 for completed historical season)
            raw_ops.insert_fixtures(fixture_data['fixtures'], 38)
            
            logger.info(f"Successfully loaded {len(fixture_data['fixtures'])} historical fixtures")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load historical fixtures: {e}")
            return False
    
    def _load_historical_stats(self, season: str) -> bool:
        """Load all historical stat categories using production scraper"""
        
        try:
            logger.info(f"Loading historical stats for {season}...")
            
            # Generate historical URLs for this season
            historical_urls = {}
            for category, pattern in self.historical_url_patterns.items():
                historical_urls[category] = pattern.format(season=season)
            
            # Initialize raw database operations
            from src.database.raw_db.connection import RawDatabaseConnection
            from src.database.raw_db.operations import RawDatabaseOperations
            
            # Create connection with custom path
            raw_conn = RawDatabaseConnection()
            raw_conn.db_path = self.historical_raw_db_path
            raw_ops = RawDatabaseOperations(raw_conn)
            
            successful_categories = []
            failed_categories = []
            total_tables_inserted = 0
            
            for i, (stat_category, url) in enumerate(historical_urls.items(), 1):
                logger.info(f"Processing {stat_category} ({i}/{len(historical_urls)})")
                
                try:
                    # Apply rate limiting (same as production)
                    if i > 1:
                        delay = 15  # 15 seconds between requests
                        logger.info(f"Rate limiting: {delay}s...")
                        time.sleep(delay)
                    
                    # Use production scraper to get stat category tables
                    category_tables = self.scraper.scrape_single_stat_category(url, stat_category)
                    
                    if not category_tables or len(category_tables) != 3:
                        logger.error(f"Failed to get 3 tables for {stat_category}")
                        failed_categories.append(stat_category)
                        continue
                    
                    # Insert each table using production raw database operations
                    category_success = True
                    for table_name, table_df in category_tables.items():
                        try:
                            # Use production insert method (same as raw pipeline)
                            raw_ops.insert_clean_stat_table(table_name, table_df, 38)  # gameweek 38
                            total_tables_inserted += 1
                            logger.info(f"  Inserted {table_name}: {len(table_df)} rows")
                        except Exception as e:
                            logger.error(f"Failed to insert {table_name}: {e}")
                            category_success = False
                    
                    if category_success:
                        successful_categories.append(stat_category)
                        logger.info(f"Successfully processed {stat_category}: 3 tables inserted")
                    else:
                        failed_categories.append(stat_category)
                        logger.error(f"{stat_category} partial failure")
                        
                except Exception as e:
                    logger.error(f"Error processing {stat_category}: {e}")
                    failed_categories.append(stat_category)
            
            # Summary
            logger.info(f"Historical stats loading complete:")
            logger.info(f"  Successful categories ({len(successful_categories)}): {successful_categories}")
            if failed_categories:
                logger.warning(f"  Failed categories ({len(failed_categories)}): {failed_categories}")
            logger.info(f"  Total tables inserted: {total_tables_inserted}")
            
            # Success if we got most categories (80%+ success rate)
            success = len(successful_categories) >= len(historical_urls) * 0.8
            return success
            
        except Exception as e:
            logger.error(f"Failed to load historical stats: {e}")
            return False


class HistoricalAnalyticsProcessor:
    """Processes historical raw data through existing analytics pipeline"""
    
    def __init__(self):
        self.historical_raw_db_path = "data/premierleague_raw_historical.duckdb"
        self.test_analytics_db_path = "data/premierleague_analytics.duckdb"
        
        logger.info("Historical analytics processor initialized")
    
    def process_historical_to_analytics(self, season: str) -> bool:
        """
        Process historical raw data through existing AnalyticsETL pipeline
        
        Args:
            season: Season being processed
            
        Returns:
            True if successful
        """
        logger.info(f"Processing historical data through analytics pipeline...")
        
        try:
            # Step 1: Run existing AnalyticsETL against historical databases
            logger.info("Running AnalyticsETL against historical raw database...")
            
            # Create AnalyticsETL instance with custom database paths
            analytics_etl = AnalyticsETL()
            
            # Override database paths temporarily
            original_raw_path = analytics_etl.db.raw_db_path if hasattr(analytics_etl.db, 'raw_db_path') else None
            original_analytics_path = analytics_etl.db.analytics_db_path if hasattr(analytics_etl.db, 'analytics_db_path') else None
            
            # Point to historical databases
            if hasattr(analytics_etl.db, 'raw_db_path'):
                analytics_etl.db.raw_db_path = self.historical_raw_db_path
            if hasattr(analytics_etl.db, 'analytics_db_path'):
                analytics_etl.db.analytics_db_path = self.test_analytics_db_path
            
            # Run full analytics pipeline (gameweek 38 for historical)
            success = analytics_etl.run_full_pipeline(target_gameweek=38, force_refresh=True)
            
            # Restore original paths
            if original_raw_path and hasattr(analytics_etl.db, 'raw_db_path'):
                analytics_etl.db.raw_db_path = original_raw_path
            if original_analytics_path and hasattr(analytics_etl.db, 'analytics_db_path'):
                analytics_etl.db.analytics_db_path = original_analytics_path
            
            if not success:
                logger.error("AnalyticsETL pipeline failed")
                return False
            
            # Step 2: Post-process to mark records as historical
            logger.info("Post-processing: marking records as historical...")
            if not self._mark_records_as_historical(season):
                return False
            
            logger.info("Historical analytics processing completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to process historical analytics: {e}")
            return False
    
    def _mark_records_as_historical(self, season: str) -> bool:
        """Mark all newly inserted records as historical"""
        
        try:
            import duckdb
            
            with duckdb.connect(self.test_analytics_db_path) as conn:
                
                # Update all records with gameweek=38 to be historical
                tables_to_update = ['analytics_players', 'analytics_keepers', 'analytics_squads', 'analytics_opponents']
                
                for table in tables_to_update:
                    # FIXED QUERY: Mark all gameweek 38 records as historical for this season
                    result = conn.execute(f"""
                        UPDATE {table} 
                        SET is_current = false
                        WHERE gameweek = 38 AND season = ?
                    """, [season])
                    
                    # Count updated records
                    count = conn.execute(f"""
                        SELECT COUNT(*) FROM {table} 
                        WHERE season = ? AND is_current = false
                    """, [season]).fetchone()[0]
                    
                    logger.info(f"Marked {count} records as historical in {table} for {season}")
            
            logger.info(f"Successfully marked all records as historical for {season}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to mark records as historical: {e}")
            return False
    
class HistoricalDataLoader:
    """Main orchestrator for historical data loading"""
    
    def __init__(self,  season: str = None):
        self.raw_loader = HistoricalRawLoader(season)
        self.analytics_processor = HistoricalAnalyticsProcessor()
        
        logger.info("Historical data loader initialized")
    
    def load_season(self, season: str = "2024-2025") -> bool:
        """
        Load complete historical season end-to-end
        
        Args:
            season: Season to load (default: 2024-2025)
            
        Returns:
            True if successful
        """
        start_time = datetime.now()

        # Set environment variable so all components use this season
        import os
        os.environ['HISTORICAL_SEASON'] = season
        
        logger.info(f"STARTING HISTORICAL DATA LOAD: {season}")
        logger.info("=" * 60)
        
        try:
            # Create new loader instance with the specific season
            raw_loader = HistoricalRawLoader(season)  # Pass season here
            
            # Step 1: Load historical raw data (truncate & reload)
            logger.info("STEP 1: Loading historical raw data...")
            if not raw_loader.truncate_and_load_season(season):
                logger.error("Failed to load historical raw data")
                return False
            
            # Step 2: Process through analytics pipeline
            logger.info("STEP 2: Processing through analytics pipeline...")
            if not self.analytics_processor.process_historical_to_analytics(season):
                logger.error("Failed to process historical analytics")
                return False
            
            # Success
            elapsed = datetime.now() - start_time
            logger.info("=" * 60)
            logger.info(f"HISTORICAL DATA LOAD COMPLETED SUCCESSFULLY in {elapsed.total_seconds():.1f}s")
            logger.info(f"Season {season} now available in test analytics database")
            
            return True
            
        except Exception as e:
            logger.error(f"Historical data load failed: {e}")
            return False
        
        finally:
            if 'HISTORICAL_SEASON' in os.environ:
                del os.environ['HISTORICAL_SEASON']

def load_multiple_seasons(seasons: list) -> bool:
    """Load multiple historical seasons in sequence"""
    print(f"\n🏈 LOADING {len(seasons)} HISTORICAL SEASONS")
    print("=" * 60)
    
    for i, season in enumerate(seasons, 1):
        print(f"\n{'='*60}")
        print(f"SEASON {i}/{len(seasons)}: {season}")
        print(f"{'='*60}")
        
        # Create loader for this specific season
        loader = HistoricalDataLoader()
        success = loader.load_season(season)
        
        if not success:
            print(f"❌ FAILED to load {season}")
            print(f"Stopped after {i-1} successful seasons")
            return False
        
        print(f"✅ SUCCESS: {season} loaded")
        
        # Brief pause between seasons
        if i < len(seasons):
            print(f"⏳ Brief pause before next season...")
            time.sleep(2)
    
    print(f"\n🎉 ALL {len(seasons)} SEASONS LOADED SUCCESSFULLY!")
    print(f"Total seasons loaded: {len(seasons)}")
    return True

def main():
    """Load historical data for testing"""
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("MULTI-SEASON HISTORICAL DATA LOADER")
    print("=" * 50)
    print("Architecture: FBRef -> Raw Historical DB -> Existing Analytics Pipeline -> Analytics DB")
    print("Now supports loading multiple seasons automatically!")
    print("=" * 50)
    
    # Define seasons to load
    historical_seasons = [
        "2015-2016",
        "2014-2015", 
        "2013-2014",
        "2012-2013",
        "2011-2012",
        "2010-2011"
    ]

    
    print(f"\nSeasons to load: {', '.join(historical_seasons)}")
    print(f"Total seasons: {len(historical_seasons)}")
    print(f"Estimated time: ~{len(historical_seasons) * 15} minutes")
    
    # Confirm before proceeding
    response = input(f"\nLoad all {len(historical_seasons)} historical seasons? (y/N): ")
    if response.lower() != 'y':
        print("Load cancelled")
        return
    
    # Load all historical seasons
    success = load_multiple_seasons(historical_seasons)
    
    if success:
        print("\n🎉 ALL HISTORICAL SEASONS LOADED SUCCESSFULLY!")
        print("\nNext steps:")
        print("1. Query analytics database to verify all seasons are present")
        print("2. Run validation queries across all historical data")
        print("3. Test that current season runs still work normally")
    else:
        print("\n❌ HISTORICAL SEASON LOADING FAILED")
        print("Check logs for details")

if __name__ == "__main__":
    main()