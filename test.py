#!/usr/bin/env python3
"""
Live URL Test - Test scraping from actual FBRef URLs
"""
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from src.scrapers.multi_stats_scraper import MultiStatsScraper
from src.database import DatabaseConnection, DatabaseOperations

def test_live_url_scraping():
    print("=== TESTING LIVE URL SCRAPING ===")
    
    # STEP 1: Start with just ONE URL to test
    test_url = "https://fbref.com/en/comps/9/stats/Premier-League-Stats"  # Standard stats
    
    print(f"Testing single URL: {test_url}")
    
    # STEP 2: Initialize scraper
    multi_scraper = MultiStatsScraper()
    
    # STEP 3: Test single stat category first
    print(f"\n🎯 Testing single category (standard stats)...")
    
    try:
        standard_data = multi_scraper.scrape_stat_category('standard', test_url)
        
        if standard_data:
            print(f"✅ Successfully scraped standard stats from live URL!")
            for table_name, df in standard_data.items():
                print(f"  {table_name}: {df.shape[0]} rows, {df.shape[1]} columns")
        else:
            print("❌ Failed to scrape standard stats")
            return
            
    except Exception as e:
        print(f"❌ Error scraping live URL: {e}")
        return
    
    # STEP 4: If single URL works, test multiple categories
    print(f"\n🚀 Single URL works! Now testing multiple categories...")
    
    # Test with 2 categories (standard + passing) from live URLs
    live_sources = {
        'standard': 'https://fbref.com/en/comps/9/stats/Premier-League-Stats',
        'passing': 'https://fbref.com/en/comps/9/passing/Premier-League-Stats'
    }
    
    print(f"Will scrape: {list(live_sources.keys())}")
    print(f"With 10 second delays between requests...")
    
    try:
        # This will use the delays from config/scraping.yaml
        all_data = multi_scraper.scrape_multiple_categories(live_sources)
        
        print(f"\n✅ Multi-category scraping results:")
        for table_name, df in all_data.items():
            print(f"  {table_name}: {df.shape[0]} rows, {df.shape[1]} columns")
        
        # STEP 5: Database test (optional)
        user_input = input(f"\nTest database insertion? (y/n): ")
        if user_input.lower() == 'y':
            test_database_insertion(all_data)
        
        print(f"\n🎉 Live URL scraping successful!")
        print(f"   Ready to scale to all 11 stat categories!")
        
    except Exception as e:
        print(f"❌ Multi-category scraping failed: {e}")

def test_database_insertion(all_data):
    """Test database insertion with live data"""
    print(f"\n💾 Testing database insertion...")
    
    # Delete existing database
    db_file = Path("data/premierleague.duckdb")
    if db_file.exists():
        db_file.unlink()
        print("🗑️  Deleted existing database")
    
    # Initialize database
    db_connection = DatabaseConnection()
    db_operations = DatabaseOperations(db_connection)
    db_operations.create_all_tables()
    
    # Create gameweek
    gameweek = 1
    db_operations.create_gameweek(gameweek)
    
    # Insert all tables
    success_count = 0
    for table_name, df in all_data.items():
        try:
            db_operations.insert_data(table_name, df, gameweek)
            print(f"  ✅ {table_name}: {len(df)} rows inserted")
            success_count += 1
        except Exception as e:
            print(f"  ❌ {table_name}: {e}")
    
    print(f"Database insertion: {success_count}/{len(all_data)} tables successful")

if __name__ == "__main__":
    test_live_url_scraping()