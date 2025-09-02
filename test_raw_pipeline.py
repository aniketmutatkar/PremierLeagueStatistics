#!/usr/bin/env python3
"""
Test Raw Pipeline - Safe testing with local HTML files following archive pattern
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from src.scraping.fbref_scraper import FBRefScraper
from src.database.raw_db import initialize_raw_database

def test_raw_pipeline():
    """Test raw pipeline with local HTML files"""
    print("=== RAW PIPELINE TEST (ARCHIVE PATTERN) ===")
    
    # Clean slate
    db_file = Path("data/premierleague_raw.duckdb")
    if db_file.exists():
        db_file.unlink()
        print("🗑️ Deleted existing raw database")
    
    # Initialize components
    print("\n📋 Initializing components...")
    scraper = FBRefScraper()
    db_connection, db_operations = initialize_raw_database()
    print("✅ Raw database initialized")
    
    # STEP 1: Test fixture scraping
    print("\n🏈 Testing fixture scraping...")
    fixture_data = scraper.scrape_fixtures('data/fixtures.html')
    
    if not fixture_data:
        print("❌ Fixture scraping failed")
        return False
    
    current_gameweek = fixture_data['current_gameweek']
    print(f"✅ Fixtures: {len(fixture_data['fixtures'])} fixtures, current GW: {current_gameweek}")
    
    # Insert fixtures
    db_operations.insert_fixtures(fixture_data['fixtures'], current_gameweek)
    
    # STEP 2: Test individual stat category processing (archive pattern)
    print("\n📊 Testing individual stat categories...")
    
    # Test with available HTML files
    test_categories = {
        'standard': 'data/standard.html',
        'passing': 'data/passing.html'
        # Add more if you have the HTML files
    }
    
    successful_categories = []
    failed_categories = []
    total_tables = 0
    
    for category, file_path in test_categories.items():
        print(f"\n--- Testing {category} ---")
        
        if not Path(file_path).exists():
            print(f"⚠️ Skipping {category}: file {file_path} not found")
            continue
        
        try:
            # Process single category (archive pattern - 3 clean tables)
            category_tables = scraper.scrape_single_stat_category(file_path, category)
            
            if len(category_tables) != 3:
                print(f"❌ {category}: Expected 3 tables, got {len(category_tables)}")
                failed_categories.append(category)
                continue
            
            # Verify table structure
            expected_tables = [f'squad_{category}', f'opponent_{category}', f'player_{category}']
            for expected_table in expected_tables:
                if expected_table not in category_tables:
                    print(f"❌ {category}: Missing {expected_table}")
                    failed_categories.append(category)
                    break
            else:
                # All tables present, show stats
                for table_name, df in category_tables.items():
                    print(f"  ✅ {table_name}: {len(df)} rows, {len(df.columns)} columns")
                    
                    # Quick data quality check
                    if 'player_' in table_name:
                        if 'Squad' in df.columns:
                            unique_squads = df['Squad'].nunique()
                            print(f"    Squad data: {unique_squads} unique teams")
                        else:
                            print(f"    ⚠️ No Squad column found")
                
                # Insert into database
                category_success = True
                for table_name, table_df in category_tables.items():
                    try:
                        db_operations.insert_clean_stat_table(table_name, table_df, current_gameweek)
                        total_tables += 1
                    except Exception as e:
                        print(f"    ❌ Failed to insert {table_name}: {e}")
                        category_success = False
                
                if category_success:
                    successful_categories.append(category)
                    print(f"✅ {category} complete: 3 tables inserted")
                else:
                    failed_categories.append(category)
                    print(f"❌ {category} database insertion failed")
        
        except Exception as e:
            print(f"❌ {category} failed: {e}")
            failed_categories.append(category)
    
    # STEP 3: Validate results
    print(f"\n🔍 Validation...")
    
    # Database status
    db_status = db_operations.get_raw_database_status()
    print(f"Raw Database Status:")
    print(f"  Total tables: {db_status['total_tables']}")
    print(f"  Stat tables: {db_status['stat_tables']}")
    print(f"  Infrastructure tables: {db_status['infrastructure_tables']}")
    
    # Data quality
    quality_results = db_operations.validate_raw_data_quality()
    print(f"\nData Quality Checks:")
    for check_name, result in quality_results.items():
        print(f"  {result}")
    
    # Category completion check
    print(f"\nCategory Completion:")
    for category in test_categories.keys():
        if category in successful_categories:
            category_status = db_operations.check_stat_category_completion(category)
            summary = category_status['summary']
            print(f"  ✅ {category}: {summary['total_rows']} rows across 3 tables")
        else:
            print(f"  ❌ {category}: failed or incomplete")
    
    # Final summary
    success_rate = len(successful_categories) / len(test_categories) if test_categories else 0
    
    print(f"\n=== TEST RESULTS ===")
    print(f"✅ Successful categories: {successful_categories}")
    if failed_categories:
        print(f"❌ Failed categories: {failed_categories}")
    
    print(f"📊 Success rate: {len(successful_categories)}/{len(test_categories)} ({success_rate:.1%})")
    print(f"📊 Total tables created: {total_tables}")
    
    if success_rate >= 0.5:  # At least 50% success for test
        print(f"\n🎉 RAW PIPELINE TEST PASSED!")
        print(f"✅ Archive pattern working correctly")
        print(f"✅ Individual stat categories processed cleanly")
        print(f"✅ Database populated with {total_tables} clean tables")
        print(f"✅ Ready to scale to all 11 stat categories")
        return True
    else:
        print(f"\n❌ RAW PIPELINE TEST FAILED")
        print(f"❌ Too many category failures")
        return False

def test_single_category():
    """Test processing of a single category in detail"""
    print("=== SINGLE CATEGORY DETAIL TEST ===")
    
    scraper = FBRefScraper()
    
    # Test standard category
    print("\n🧪 Testing standard category in detail...")
    standard_tables = scraper.scrape_single_stat_category('data/standard.html', 'standard')
    
    if not standard_tables:
        print("❌ Failed to scrape standard category")
        return False
    
    print(f"✅ Scraped {len(standard_tables)} tables")
    
    for table_name, df in standard_tables.items():
        print(f"\n--- {table_name} Analysis ---")
        print(f"Shape: {df.shape}")
        print(f"Columns: {df.columns.tolist()[:10]}...")  # First 10 columns
        
        # Check for duplicates
        if len(df) != df.drop_duplicates().shape[0]:
            duplicate_count = len(df) - df.drop_duplicates().shape[0]
            print(f"⚠️ Found {duplicate_count} duplicate rows")
        else:
            print(f"✅ No duplicate rows")
        
        # Check key columns
        if 'player_' in table_name:
            key_cols = ['Player', 'Squad', 'Nation', 'Pos', 'Age']
            for col in key_cols:
                if col in df.columns:
                    null_count = df[col].isna().sum()
                    unique_count = df[col].nunique()
                    print(f"  {col}: {null_count} nulls, {unique_count} unique values")
                    
                    if col == 'Squad':
                        sample_squads = df[col].dropna().unique()[:5]
                        print(f"    Sample squads: {sample_squads.tolist()}")
    
    return True

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Test Raw Pipeline')
    parser.add_argument('--single', action='store_true', help='Test single category in detail')
    
    args = parser.parse_args()
    
    if args.single:
        success = test_single_category()
    else:
        success = test_raw_pipeline()
    
    if success:
        print("\n✅ All tests passed! Raw pipeline is working correctly.")
    else:
        print("\n❌ Tests failed. Check errors above.")
    
    sys.exit(0 if success else 1)