#!/usr/bin/env python3
"""
Test database insertion with the extracted data
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from src.scrapers.stats_scraper import StatsScraper
from src.database import DatabaseConnection, DatabaseOperations

def test_database_insertion():
    print("=== TESTING DATABASE INSERTION ===")
    
    # Get clean data
    scraper = StatsScraper()
    stats_data = scraper.scrape_stats('data/standard.html')
    print(f"✅ Extracted {len(stats_data)} tables")
    
    # Show what we extracted
    for name, df in stats_data.items():
        print(f"  {name}: {df.shape[0]} rows, {df.shape[1]} columns")
    
    # Initialize database (NO create_all_tables call)
    print("\nInitializing database...")
    db_connection = DatabaseConnection()
    db_operations = DatabaseOperations(db_connection) 
    print("✅ Database initialized")
    
    # Test inserting each table
    gameweek = 1
    results = {}
    
    for table_name, df in stats_data.items():
        print(f"\nInserting {table_name}...")
        try:
            db_operations.insert_data(table_name, df, gameweek)
            print(f"  ✅ SUCCESS: {len(df)} rows inserted")
            results[table_name] = 'success'
        except Exception as e:
            print(f"  ❌ FAILED: {e}")
            results[table_name] = f'failed: {e}'
    
    print(f"\n=== RESULTS ===")
    for table, result in results.items():
        print(f"  {table}: {result}")
    
    return results

if __name__ == "__main__":
    test_database_insertion()