#!/usr/bin/env python3
"""
Column Mismatch Diagnostic Script
Compares expected database schema columns vs actual consolidation output columns
"""

import sys
sys.path.append('.')

import duckdb
import pandas as pd
from src.analytics.etl.player_consolidation import PlayerDataConsolidator

def diagnose_column_mismatch():
    """Diagnose the exact column mismatch between schema and consolidation"""
    
    print("🔍 COLUMN MISMATCH DIAGNOSTIC")
    print("=" * 60)
    
    # Step 1: Get expected schema columns from database
    print("\n📋 STEP 1: Getting expected schema columns...")
    try:
        with duckdb.connect('data/premierleague_analytics.duckdb') as conn:
            # Get analytics_players schema
            outfield_schema = conn.execute("PRAGMA table_info(analytics_players)").fetchall()
            outfield_expected_cols = [row[1] for row in outfield_schema]  # Column names are in index 1
            
            # Get analytics_keepers schema  
            keeper_schema = conn.execute("PRAGMA table_info(analytics_keepers)").fetchall()
            keeper_expected_cols = [row[1] for row in keeper_schema]
            
            print(f"✅ Expected outfield columns: {len(outfield_expected_cols)}")
            print(f"✅ Expected keeper columns: {len(keeper_expected_cols)}")
            
    except Exception as e:
        print(f"❌ Error reading schema: {e}")
        return False
    
    # Step 2: Get actual consolidation output columns
    print("\n🔄 STEP 2: Running consolidation to get actual columns...")
    try:
        # Initialize consolidator
        consolidator = PlayerDataConsolidator()
        
        # Connect directly to raw database
        with duckdb.connect('data/premierleague_raw.duckdb', read_only=True) as raw_conn:
            # Run consolidation for gameweek 5
            outfield_df, goalkeepers_df = consolidator.consolidate_players(raw_conn, 5)
            
            outfield_actual_cols = list(outfield_df.columns)
            keeper_actual_cols = list(goalkeepers_df.columns)
            
            print(f"✅ Actual outfield columns: {len(outfield_actual_cols)}")
            print(f"✅ Actual keeper columns: {len(keeper_actual_cols)}")
            
    except Exception as e:
        print(f"❌ Error running consolidation: {e}")
        return False
    
    # Step 3: Compare outfield columns
    print("\n🆚 STEP 3: Comparing outfield columns...")
    print(f"Expected: {len(outfield_expected_cols)} | Actual: {len(outfield_actual_cols)}")
    
    # Find missing columns (in schema but not in consolidation)
    missing_cols = set(outfield_expected_cols) - set(outfield_actual_cols)
    # Find extra columns (in consolidation but not in schema)
    extra_cols = set(outfield_actual_cols) - set(outfield_expected_cols)
    
    if missing_cols:
        print(f"❌ MISSING columns (in schema but not consolidation): {len(missing_cols)}")
        for col in sorted(missing_cols):
            print(f"   - {col}")
    
    if extra_cols:
        print(f"⚠️  EXTRA columns (in consolidation but not schema): {len(extra_cols)}")
        for col in sorted(extra_cols):
            print(f"   - {col}")
    
    if not missing_cols and not extra_cols:
        print("✅ Outfield columns match perfectly!")
    
    # Step 4: Compare goalkeeper columns
    print("\n🥅 STEP 4: Comparing goalkeeper columns...")
    print(f"Expected: {len(keeper_expected_cols)} | Actual: {len(keeper_actual_cols)}")
    
    # Find missing columns (in schema but not in consolidation)
    keeper_missing_cols = set(keeper_expected_cols) - set(keeper_actual_cols)
    # Find extra columns (in consolidation but not in schema)
    keeper_extra_cols = set(keeper_actual_cols) - set(keeper_expected_cols)
    
    if keeper_missing_cols:
        print(f"❌ MISSING keeper columns (in schema but not consolidation): {len(keeper_missing_cols)}")
        for col in sorted(keeper_missing_cols):
            print(f"   - {col}")
    
    if keeper_extra_cols:
        print(f"⚠️  EXTRA keeper columns (in consolidation but not schema): {len(keeper_extra_cols)}")
        for col in sorted(keeper_extra_cols):
            print(f"   - {col}")
    
    if not keeper_missing_cols and not keeper_extra_cols:
        print("✅ Goalkeeper columns match perfectly!")
    
    # Step 5: Detailed column order comparison (for outfield)
    print("\n📝 STEP 5: Detailed outfield column order analysis...")
    
    # Show first 10 columns of each to see any patterns
    print("\nFirst 10 expected columns:")
    for i, col in enumerate(outfield_expected_cols[:10]):
        print(f"   {i+1:2d}. {col}")
    
    print("\nFirst 10 actual columns:")
    for i, col in enumerate(outfield_actual_cols[:10]):
        print(f"   {i+1:2d}. {col}")
    
    # Step 6: Show exact mismatch summary
    print("\n📊 SUMMARY:")
    print(f"Outfield: Expected {len(outfield_expected_cols)}, Got {len(outfield_actual_cols)}, Diff: {len(outfield_expected_cols) - len(outfield_actual_cols)}")
    print(f"Keepers:  Expected {len(keeper_expected_cols)}, Got {len(keeper_actual_cols)}, Diff: {len(keeper_expected_cols) - len(keeper_actual_cols)}")
    
    if missing_cols or extra_cols or keeper_missing_cols or keeper_extra_cols:
        print("\n🔧 ACTION NEEDED:")
        if missing_cols:
            print(f"   - Add these {len(missing_cols)} columns to consolidation: {sorted(missing_cols)}")
        if extra_cols:
            print(f"   - Remove these {len(extra_cols)} columns from consolidation: {sorted(extra_cols)}")
        if keeper_missing_cols:
            print(f"   - Add these {len(keeper_missing_cols)} keeper columns to consolidation: {sorted(keeper_missing_cols)}")
        if keeper_extra_cols:
            print(f"   - Remove these {len(keeper_extra_cols)} keeper columns from consolidation: {sorted(keeper_extra_cols)}")
    else:
        print("✅ All columns match - the issue might be column ordering!")
    
    return True

if __name__ == "__main__":
    success = diagnose_column_mismatch()
    exit(0 if success else 1)