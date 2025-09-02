#!/usr/bin/env python3
"""
Raw Data Pipeline - Following archive pattern exactly
Processes each stat category individually, no consolidation
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
    """Main raw data pipeline following archive pattern"""
    print("=== RAW DATA PIPELINE (ARCHIVE PATTERN) ===")
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
        
        current_gameweek = fixture_data['current_gameweek']
        print(f"✅ Current gameweek detected: {current_gameweek}")
        
        # Insert fixtures
        db_operations.insert_fixtures(fixture_data['fixtures'], current_gameweek)
        
        # STEP 2: Process each stat category individually (archive pattern)
        print(f"\n📊 Processing stat categories individually...")
        
        # Get stat categories from config
        stat_categories = list(scraper.sources_config['stats_sources'].keys())
        print(f"Processing {len(stat_categories)} stat categories: {stat_categories}")
        
        successful_categories = []
        failed_categories = []
        total_tables_inserted = 0
        
        for i, stat_category in enumerate(stat_categories, 1):
            print(f"\n--- Processing {stat_category} ({i}/{len(stat_categories)}) ---")
            
            try:
                # Get source for this category
                category_config = scraper.sources_config['stats_sources'][stat_category]
                source_url = category_config['url']
                
                # Apply rate limiting (archive pattern)
                if i > 1:  # Not first category
                    delay = scraper.scraping_config['scraping']['delays']['between_requests']
                    print(f"Rate limiting: waiting {delay}s...")
                    time.sleep(delay)
                
                # Scrape single category (returns 3 clean tables)
                category_tables = scraper.scrape_single_stat_category(source_url, stat_category)
                
                if not category_tables or len(category_tables) != 3:
                    logger.error(f"Failed to get 3 tables for {stat_category}")
                    failed_categories.append(stat_category)
                    continue
                
                # Insert each table individually (archive pattern)
                category_success = True
                for table_name, table_df in category_tables.items():
                    try:
                        db_operations.insert_clean_stat_table(table_name, table_df, current_gameweek)
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
        print(f"\n🔍 Pipeline completion summary...")
        print(f"✅ Successful categories ({len(successful_categories)}): {successful_categories}")
        if failed_categories:
            print(f"❌ Failed categories ({len(failed_categories)}): {failed_categories}")
        
        print(f"📊 Total tables inserted: {total_tables_inserted}")
        
        # Get database status
        db_status = db_operations.get_raw_database_status()
        print(f"📈 Database status:")
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
            print(f"\n🎉 RAW PIPELINE COMPLETE!")
            print(f"✅ {len(successful_categories)}/{len(stat_categories)} categories successful")
            print(f"✅ {total_tables_inserted} tables populated with clean data")
            print(f"✅ Data through gameweek {current_gameweek}")
            print(f"✅ Ready for analytics layer")
        else:
            print(f"\n⚠️ PIPELINE COMPLETED WITH ISSUES")
            print(f"⚠️ Only {len(successful_categories)}/{len(stat_categories)} categories successful")
            print(f"⚠️ Check failed categories: {failed_categories}")
        
        return success
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        print(f"\n❌ PIPELINE FAILED: {e}")
        return False

def check_raw_pipeline_status():
    """Check current raw pipeline status"""
    print("=== RAW PIPELINE STATUS CHECK ===")
    
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
        return False

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Raw Data Pipeline')
    parser.add_argument('--status', action='store_true', help='Check pipeline status only')
    parser.add_argument('--force', action='store_true', help='Force run even if data exists')
    
    args = parser.parse_args()
    
    if args.status:
        success = check_raw_pipeline_status()
    else:
        success = run_raw_pipeline()
    
    sys.exit(0 if success else 1)