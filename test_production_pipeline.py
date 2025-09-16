#!/usr/bin/env python3
"""
Test Production Analytics Pipeline
Test the complete production ETL pipeline
"""

import sys
from pathlib import Path

# Add src to path 
sys.path.append(str(Path(__file__).parent / 'src'))

from analytics.pipelines.analytics_pipeline import AnalyticsETLPipeline
from database.analytics_db import AnalyticsDBConnection

def test_production_pipeline():
    """Test the complete production pipeline"""
    print("🧪 TESTING PRODUCTION ANALYTICS PIPELINE")
    print("=" * 50)
    
    # Initialize pipeline
    pipeline = AnalyticsETLPipeline()
    db = AnalyticsDBConnection()
    
    # Test 1: Check connections
    print("1️⃣ Testing database connections...")
    raw_ok, analytics_ok = db.validate_connections()
    
    if not (raw_ok and analytics_ok):
        print("❌ Database connections failed")
        return False
    
    print("✅ Database connections OK")
    
    # Test 2: Check current state
    print("\n2️⃣ Checking current state...")
    current_gw = db.get_current_gameweek()
    
    with db.get_analytics_connection() as conn:
        analytics_count = conn.execute("SELECT COUNT(*) FROM analytics_players").fetchone()[0]
    
    print(f"   Raw gameweek: {current_gw}")
    print(f"   Analytics records: {analytics_count}")
    
    # Test 3: Run pipeline (force refresh to test functionality)
    print(f"\n3️⃣ Running pipeline for gameweek {current_gw}...")
    print("   (Using force_refresh=True to test full functionality)")
    
    success = pipeline.run_full_pipeline(
        target_gameweek=current_gw,
        force_refresh=True  # Force refresh to test the pipeline
    )
    
    if not success:
        print("❌ Pipeline execution failed")
        return False
    
    print("✅ Pipeline execution completed")
    
    # Test 4: Validate results
    print("\n4️⃣ Validating results...")
    
    with db.get_analytics_connection() as conn:
        # Check record count
        new_analytics_count = conn.execute("SELECT COUNT(*) FROM analytics_players").fetchone()[0]
        
        # Check current records
        current_count = conn.execute("""
            SELECT COUNT(*) FROM analytics_players WHERE is_current = true
        """).fetchone()[0]
        
        # Check gameweek
        analytics_gw = conn.execute("""
            SELECT MAX(gameweek) FROM analytics_players WHERE is_current = true
        """).fetchone()[0]
        
        # Check unique players
        unique_players = conn.execute("""
            SELECT COUNT(DISTINCT player_id) FROM analytics_players WHERE is_current = true
        """).fetchone()[0]
    
    print(f"   Total analytics records: {new_analytics_count}")
    print(f"   Current records: {current_count}")
    print(f"   Analytics gameweek: {analytics_gw}")
    print(f"   Unique players: {unique_players}")
    
    # Validation checks
    if analytics_gw != current_gw:
        print(f"❌ Analytics gameweek mismatch: expected {current_gw}, got {analytics_gw}")
        return False
    
    if current_count < 300:
        print(f"❌ Too few current records: {current_count}")
        return False
    
    # For transfers, unique players can be less than total records (same player, different teams)
    if unique_players > current_count:
        print(f"❌ More unique players than records: {unique_players} vs {current_count}")
        return False
    
    if current_count - unique_players > 5:  # Allow reasonable number of transfers
        print(f"⚠️  Many player transfers detected: {current_count - unique_players} players with multiple teams")
    
    print("✅ All validations passed")
    
    # Test 5: Check derived metrics
    print("\n5️⃣ Checking derived metrics...")
    
    with db.get_analytics_connection() as conn:
        # Check if derived metrics exist
        sample_metrics = conn.execute("""
            SELECT player_name, goals_vs_expected, progressive_actions_per_90, form_score
            FROM analytics_players 
            WHERE is_current = true 
            LIMIT 3
        """).fetchall()
    
    if sample_metrics:
        print("   Sample derived metrics:")
        for player, goals_vs_exp, prog_per_90, form in sample_metrics:
            print(f"      {player}: goals_vs_exp={goals_vs_exp:.2f}, prog_per_90={prog_per_90:.2f}")
        print("✅ Derived metrics are populated")
    else:
        print("❌ No derived metrics found")
        return False
    
    print("\n🎉 PRODUCTION PIPELINE TEST PASSED!")
    print("📝 Analytics layer is working correctly and ready for use")
    
    return True

if __name__ == "__main__":
    success = test_production_pipeline()
    sys.exit(0 if success else 1)