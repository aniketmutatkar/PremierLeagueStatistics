#!/usr/bin/env python3
"""
Historical Data Loader - FIXED VERSION
One-time loader for all historical seasons (2010-2011 through 2024-2025)

FIXES:
- Removed truncation bug (now accumulates seasons instead of deleting)
- Fixed fixtures DataFrame column mapping
- Added skip-if-exists logic
- Loads all 15 historical seasons with rate limiting

Architecture:
1. Load historical fixtures into premierleague_raw_historical.duckdb
2. Load historical stats (squads, players, keepers) if needed
3. Transform and load into analytics database
"""

import logging
import sys
import time
import duckdb
from pathlib import Path
from datetime import datetime
from typing import List, Optional

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.scraping.fbref_scraper import FBRefScraper
from src.analytics.fixtures import FixturesProcessor

logger = logging.getLogger(__name__)


class HistoricalDataLoader:
    """Comprehensive historical data loader for all seasons"""

    # All historical seasons to load
    ALL_SEASONS = [
        "2024-2025", "2023-2024", "2022-2023", "2021-2022", "2020-2021",
        "2019-2020", "2018-2019", "2017-2018", "2016-2017", "2015-2016",
        "2014-2015", "2013-2014", "2012-2013", "2011-2012", "2010-2011"
    ]

    def __init__(self, rate_limit_seconds: int = 20):
        self.historical_raw_db_path = "data/historical/premierleague_raw_historical.duckdb"
        self.analytics_db_path = "data/premierleague_analytics.duckdb"
        self.rate_limit_seconds = rate_limit_seconds

        # Create database directory
        Path(self.historical_raw_db_path).parent.mkdir(parents=True, exist_ok=True)

        logger.info("Historical data loader initialized")
        logger.info(f"Rate limiting: {rate_limit_seconds}s between seasons")

    def initialize_raw_fixtures_table(self):
        """Create raw_fixtures table if it doesn't exist (NO TRUNCATION)"""
        try:
            logger.info("Initializing raw_fixtures table (if needed)...")

            with duckdb.connect(self.historical_raw_db_path) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS raw_fixtures (
                        gameweek DOUBLE,
                        day_of_week VARCHAR,
                        match_date TIMESTAMP,
                        match_time VARCHAR,
                        home_team VARCHAR,
                        xG DOUBLE,
                        score VARCHAR,
                        xG_away DOUBLE,
                        away_team VARCHAR,
                        attendance DOUBLE,
                        venue VARCHAR,
                        referee VARCHAR,
                        match_report VARCHAR,
                        notes DOUBLE,
                        home_score DOUBLE,
                        away_score DOUBLE,
                        is_completed BOOLEAN,
                        fixture_id VARCHAR,
                        scraped_date DATE,
                        last_updated DATE,
                        season VARCHAR
                    )
                """)

                # Check current state
                count = conn.execute("SELECT COUNT(*) FROM raw_fixtures").fetchone()[0]
                logger.info(f"✅ Fixtures table ready. Current records: {count:,}")

            return True

        except Exception as e:
            logger.error(f"Failed to initialize fixtures table: {e}")
            return False

    def check_season_already_loaded(self, season: str) -> bool:
        """Check if season already exists in raw_fixtures"""
        try:
            with duckdb.connect(self.historical_raw_db_path) as conn:
                count = conn.execute(
                    "SELECT COUNT(*) FROM raw_fixtures WHERE season = ?",
                    [season]
                ).fetchone()[0]

                if count > 0:
                    logger.info(f"⏭️  Season {season} already loaded ({count} fixtures)")
                    return True
                return False

        except Exception as e:
            logger.warning(f"Error checking if season exists: {e}")
            return False

    def load_fixtures_for_season(self, season: str) -> bool:
        """Load fixtures for a single season"""
        try:
            logger.info("="*80)
            logger.info(f"Loading fixtures for season: {season}")
            logger.info("="*80)

            # Scrape fixtures
            logger.info("🌐 Scraping fixtures from FBRef...")
            scraper = FBRefScraper(override_season=season)
            fixtures_url = f"https://fbref.com/en/comps/9/{season}/schedule/{season}-Premier-League-Scores-and-Fixtures"
            logger.info(f"   URL: {fixtures_url}")
            logger.info(f"   Waiting 3s before request...")
            time.sleep(3)

            fixture_data = scraper.scrape_fixtures(fixtures_url)

            if not fixture_data or 'fixtures' not in fixture_data:
                logger.error(f"❌ Failed to scrape fixtures for {season}")
                return False

            fixtures_df = fixture_data['fixtures']
            logger.info(f"✅ Scraped {len(fixtures_df)} fixtures")

            # Rename DataFrame columns to match database schema
            if hasattr(fixtures_df, 'rename'):
                fixtures_df = fixtures_df.rename(columns={
                    'xG.1': 'xG_away',
                    'Match Report': 'match_report',
                    'Notes': 'notes'
                })

            # Convert to list of dicts and add season
            fixtures_list = fixtures_df.to_dict('records')
            for fixture in fixtures_list:
                fixture['season'] = season

            # Insert into database
            logger.info(f"💾 Inserting {len(fixtures_list)} fixtures into database...")
            self._insert_fixtures(fixtures_list)

            logger.info(f"✅ Successfully loaded {len(fixtures_list)} fixtures for {season}")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to load fixtures for {season}: {e}")
            import traceback
            traceback.print_exc()
            return False

    def _insert_fixtures(self, fixtures_list: List[dict]):
        """Insert fixtures into database"""
        try:
            with duckdb.connect(self.historical_raw_db_path) as conn:
                for fixture in fixtures_list:
                    # Build column names and placeholders dynamically
                    columns = list(fixture.keys())
                    placeholders = ', '.join(['?' for _ in columns])
                    column_names = ', '.join(columns)
                    values = [fixture[col] for col in columns]

                    query = f"INSERT INTO raw_fixtures ({column_names}) VALUES ({placeholders})"
                    conn.execute(query, values)

        except Exception as e:
            logger.error(f"Failed to insert fixtures: {e}")
            raise

    def load_all_seasons(self, skip_if_exists: bool = True) -> bool:
        """Load all 15 historical seasons"""
        logger.info("="*80)
        logger.info("LOADING ALL HISTORICAL SEASONS")
        logger.info("="*80)
        logger.info(f"Seasons: {len(self.ALL_SEASONS)} total")
        logger.info(f"Rate limiting: {self.rate_limit_seconds}s between seasons")
        logger.info(f"Estimated time: ~{len(self.ALL_SEASONS) * 25 // 60} minutes")
        logger.info("="*80)

        # Initialize table
        if not self.initialize_raw_fixtures_table():
            return False

        loaded = []
        skipped = []
        failed = []

        for i, season in enumerate(self.ALL_SEASONS, 1):
            logger.info(f"\n{'#'*80}")
            logger.info(f"SEASON {i}/{len(self.ALL_SEASONS)}: {season}")
            logger.info(f"Progress: {len(loaded)} complete, {len(self.ALL_SEASONS) - i} remaining")
            logger.info(f"{'#'*80}")

            # Check if already loaded
            if skip_if_exists and self.check_season_already_loaded(season):
                skipped.append(season)
                continue

            # Load fixtures
            if self.load_fixtures_for_season(season):
                loaded.append(season)
                logger.info(f"✅ {season} LOADED")
            else:
                failed.append(season)
                logger.error(f"❌ {season} FAILED")
                logger.warning(f"Season {season} failed. Continuing with remaining seasons...")

            # Rate limiting between seasons
            if i < len(self.ALL_SEASONS):
                logger.info(f"⏳ Rate limiting: {self.rate_limit_seconds}s before next season...")
                time.sleep(self.rate_limit_seconds)

            # Checkpoint every 5 seasons
            if i % 5 == 0:
                logger.info("\n📊 CHECKPOINT - Verifying database...")
                self.verify_database()

        # Final summary
        logger.info("\n" + "="*80)
        logger.info("HISTORICAL SEASONS LOAD COMPLETE")
        logger.info("="*80)
        logger.info(f"✅ Loaded: {len(loaded)} seasons")
        if skipped:
            logger.info(f"⏭️  Skipped: {len(skipped)} seasons (already existed)")
        if failed:
            logger.warning(f"❌ Failed: {len(failed)} seasons - {failed}")

        self.verify_database()

        return len(failed) == 0

    def verify_database(self):
        """Verify database contents"""
        try:
            with duckdb.connect(self.historical_raw_db_path) as conn:
                total = conn.execute("SELECT COUNT(*) FROM raw_fixtures").fetchone()[0]
                seasons = conn.execute("""
                    SELECT season, COUNT(*) as count
                    FROM raw_fixtures
                    GROUP BY season
                    ORDER BY season DESC
                """).fetchall()

                logger.info(f"   Total fixtures: {total:,}")
                logger.info(f"   Seasons in database: {len(seasons)}")
                for season, count in seasons[:5]:  # Show first 5
                    logger.info(f"      {season}: {count} fixtures")

        except Exception as e:
            logger.warning(f"Could not verify database: {e}")

    def load_to_analytics(self):
        """Transform and load all fixtures to analytics database"""
        logger.info("\n" + "="*80)
        logger.info("LOADING TO ANALYTICS DATABASE")
        logger.info("="*80)

        try:
            # Load all fixtures from raw
            with duckdb.connect(self.historical_raw_db_path) as conn:
                raw_fixtures_df = conn.execute("SELECT * FROM raw_fixtures ORDER BY season, match_date").fetchdf()

            logger.info(f"✅ Loaded {len(raw_fixtures_df):,} raw fixtures")

            # Rename columns back for transformation (transformation expects old names)
            raw_fixtures_df = raw_fixtures_df.rename(columns={
                'xG_away': 'xG.1',
                'match_report': 'Match Report',
                'notes': 'Notes'
            })

            # Transform using existing processor
            processor = FixturesProcessor()
            analytics_fixtures_df = processor._create_analytics_fixtures_dataframe(raw_fixtures_df)

            logger.info(f"✅ Transformed {len(analytics_fixtures_df):,} fixtures")

            # Load to analytics
            with duckdb.connect(self.analytics_db_path) as conn:
                # Delete historical seasons (keep 2025-2026 current season)
                seasons = raw_fixtures_df['season'].unique()
                historical_seasons = [s for s in seasons if s != '2025-2026']

                for season in historical_seasons:
                    conn.execute("DELETE FROM analytics_fixtures WHERE season = ?", [season])

                # Insert all fixtures
                conn.register('temp_fixtures', analytics_fixtures_df)
                conn.execute("INSERT INTO analytics_fixtures SELECT * FROM temp_fixtures")
                conn.unregister('temp_fixtures')

                # Verify
                total = conn.execute("SELECT COUNT(*) FROM analytics_fixtures").fetchone()[0]
                logger.info(f"✅ Analytics database now has {total:,} total fixtures")

            return True

        except Exception as e:
            logger.error(f"Failed to load to analytics: {e}")
            import traceback
            traceback.print_exc()
            return False


def main():
    """Main entry point"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    print("\n" + "="*80)
    print("HISTORICAL DATA LOADER - ALL SEASONS")
    print("="*80)
    print("Loads: 15 seasons (2010-2011 through 2024-2025)")
    print("       Fixtures for all seasons")
    print("       Transforms and loads to analytics database")
    print("="*80)

    loader = HistoricalDataLoader(rate_limit_seconds=20)

    # Step 1: Load all fixtures to raw database
    print("\n📥 STEP 1: Loading fixtures to raw database...")
    if not loader.load_all_seasons(skip_if_exists=True):
        print("\n❌ Failed to load all seasons")
        return

    # Step 2: Transform and load to analytics
    print("\n🔄 STEP 2: Transforming and loading to analytics...")
    if not loader.load_to_analytics():
        print("\n❌ Failed to load to analytics")
        return

    print("\n" + "="*80)
    print("✅ ALL HISTORICAL DATA LOADED SUCCESSFULLY!")
    print("="*80)
    print("\nYour analytics database now contains:")
    print("  - 16 seasons of fixtures (2010-2011 through 2025-2026)")
    print("  - All enriched columns (match_outcome, winner, points, etc.)")
    print("  - Ready for statistical analysis!")
    print("="*80)


if __name__ == "__main__":
    main()
