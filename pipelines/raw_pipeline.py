#!/usr/bin/env python3
"""
Raw Data Pipeline - NEW FIXTURE-BASED IMPLEMENTATION
Phase 2: Remove gameweek tagging, store raw data without gameweek metadata
"""
import sys
import logging
import time
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.scraping.fbref_scraper import FBRefScraper
from src.database.raw_db import initialize_raw_database

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_raw_pipeline():
    """NEW: Raw data pipeline without gameweek tagging"""
    print("=" * 60)
    print("RAW DATA PIPELINE (NEW FIXTURE-BASED MODE)")
    print("=" * 60)
    print(f"Run started: {datetime.now()}")
    
    try:
        # Initialize components
        print("\n📋 Initializing pipeline...")
        scraper = FBRefScraper()
        db_connection, db_operations = initialize_raw_database()
        print("✅ Pipeline components ready")
        
        # STEP 1: Process fixtures
        print("\n🏈 Processing fixtures...")
        fixtures_url = scraper.sources_config['fixtures_sources']['current_season']['url']
        fixture_data = scraper.scrape_fixtures(fixtures_url)
        
        if not fixture_data:
            logger.error("Failed to scrape fixtures")
            return False
        
        # NEW: Get gameweek status (for logging only)
        gw_status = fixture_data['gameweek_status']
        print(f"✅ Gameweek status:")
        print(f"   Min: {gw_status['min_gameweek']}, Max: {gw_status['max_gameweek']}")
        print(f"   Teams aligned: {gw_status['all_teams_aligned']}")
        if gw_status['teams_behind']:
            print(f"   Teams behind: {len(gw_status['teams_behind'])}")
        
        # NEW: Insert fixtures WITHOUT gameweek parameter
        db_operations.insert_fixtures(fixture_data['fixtures'])
        print("✅ Fixtures inserted")
        
        # STEP 2: Process each stat category individually
        print(f"\n📊 Processing stat categories...")
        
        # Get stat categories from config
        stat_categories = list(scraper.sources_config['stats_sources'].keys())
        print(f"Processing {len(stat_categories)} categories: {stat_categories}")
        
        successful_categories = []
        failed_categories = []
        total_tables_inserted = 0
        
        for i, stat_category in enumerate(stat_categories, 1):
            print(f"\n--- Processing {stat_category} ({i}/{len(stat_categories)}) ---")
            
            try:
                # Get source for this category
                category_config = scraper.sources_config['stats_sources'][stat_category]
                source_url = category_config['url']
                
                # Apply rate limiting
                if i > 1:
                    delay = scraper.scraping_config['scraping']['delays']['between_requests']
                    print(f"Rate limiting: waiting {delay}s...")
                    time.sleep(delay)
                
                # Scrape single category (returns 3 clean tables)
                category_tables = scraper.scrape_single_stat_category(source_url, stat_category)
                
                if not category_tables or len(category_tables) != 3:
                    logger.error(f"Failed to get 3 tables for {stat_category}")
                    failed_categories.append(stat_category)
                    continue
                
                # NEW: Insert each table WITHOUT gameweek parameter
                category_success = True
                for table_name, table_df in category_tables.items():
                    try:
                        db_operations.insert_clean_stat_table(table_name, table_df)
                        total_tables_inserted += 1
                        print(f"  ✅ {table_name}: {len(table_df)} rows")
                    except Exception as e:
                        logger.error(f"Failed to insert {table_name}: {e}")
                        category_success = False
                
                if category_success:
                    successful_categories.append(stat_category)
                    print(f"✅ {stat_category} complete: 3 tables inserted")
                else:
                    failed_categories.append(stat_category)
                    print(f"❌ {stat_category} partial failure")
                
            except Exception as e:
                logger.error(f"Error processing {stat_category}: {e}")
                failed_categories.append(stat_category)
                print(f"❌ {stat_category} failed: {e}")
        
        # STEP 3: Final validation
        print(f"\n{'='*60}")
        print("PIPELINE COMPLETION SUMMARY")
        print(f"{'='*60}")
        print(f"✅ Successful categories: {len(successful_categories)}/{len(stat_categories)}")
        if successful_categories:
            print(f"   {successful_categories}")
        if failed_categories:
            print(f"❌ Failed categories: {len(failed_categories)}")
            print(f"   {failed_categories}")
        
        print(f"📊 Total tables inserted: {total_tables_inserted}")
        
        # Get database status
        db_status = db_operations.get_raw_database_status()
        print(f"\n📈 Database status:")
        print(f"  Total tables: {db_status['total_tables']}")
        print(f"  Stat tables: {db_status['stat_tables']}")
        print(f"  Infrastructure tables: {db_status['infrastructure_tables']}")
        
        # Validate data quality
        quality_results = db_operations.validate_raw_data_quality()
        print(f"\n🔍 Data quality checks:")
        for check_name, result in quality_results.items():
            print(f"  {result}")
        
        # Success criteria
        success = len(successful_categories) >= len(stat_categories) * 0.8  # 80% success rate
        
        if success:
            print(f"\n{'='*60}")
            print("🎉 RAW PIPELINE COMPLETE!")
            print(f"{'='*60}")
            print(f"✅ {len(successful_categories)}/{len(stat_categories)} categories successful")
            print(f"✅ {total_tables_inserted} tables populated")
            print(f"✅ Season: {fixture_data['season']}")
            print(f"✅ Gameweek range: {gw_status['min_gameweek']}-{gw_status['max_gameweek']}")
            print(f"\n💡 NOTE: Raw data stored WITHOUT gameweek tagging")
            print(f"   Gameweek assignment will happen in analytics layer")
        else:
            print(f"\n⚠️ PIPELINE COMPLETED WITH ISSUES")
            print(f"⚠️ Only {len(successful_categories)}/{len(stat_categories)} categories successful")
            print(f"⚠️ Check failed categories: {failed_categories}")
        
        return success
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        print(f"\n❌ PIPELINE FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

def check_raw_pipeline_status():
    """Check current raw pipeline status"""
    print("=" * 60)
    print("RAW PIPELINE STATUS CHECK")
    print("=" * 60)
    
    try:
        db_connection, db_operations = initialize_raw_database()
        
        # Get database status
        db_status = db_operations.get_raw_database_status()
        quality_results = db_operations.validate_raw_data_quality()
        
        print(f"\nRaw Database Status:")
        print(f"  Total tables: {db_status['total_tables']}")
        print(f"  Stat tables: {db_status['stat_tables']}")
        print(f"  Infrastructure tables: {db_status['infrastructure_tables']}")
        
        # Show stat tables by category
        expected_categories = ['standard', 'keepers', 'keepersadv', 'shooting', 'passing', 
                              'passingtypes', 'goalshotcreation', 'defense', 'possession', 
                              'playingtime', 'misc']
        
        print(f"\nStat Category Status:")
        for category in expected_categories:
            category_status = db_operations.check_stat_category_completion(category)
            summary = category_status['summary']
            
            if summary['all_tables_exist']:
                print(f"  ✅ {category}: {summary['total_rows']} rows across 3 tables")
            else:
                print(f"  ❌ {category}: incomplete")
        
        print(f"\nData Quality:")
        for check_name, result in quality_results.items():
            print(f"  {result}")
        
        return True
        
    except Exception as e:
        print(f"Status check failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Raw Data Pipeline (NEW Fixture-Based)')
    parser.add_argument('--status', action='store_true', help='Check pipeline status only')
    parser.add_argument('--force', action='store_true', help='Force run (currently no effect)')
    
    args = parser.parse_args()
    
    if args.status:
        success = check_raw_pipeline_status()
    else:
        success = run_raw_pipeline()
    
    sys.exit(0 if success else 1)