#!/usr/bin/env python3
"""
Test Analytics Foundation
Quick test to validate our analytics database setup works
"""

import sys
from pathlib import Path

# Add src to path so we can import our modules
sys.path.append(str(Path(__file__).parent / 'src'))

from database.analytics_db import AnalyticsDBConnection, AnalyticsDBOperations

def test_analytics_foundation():
    """Test that our analytics foundation is working"""
    print("🧪 Testing Analytics Foundation...")
    
    # Test 1: Connection validation
    print("\n1️⃣ Testing database connections...")
    db = AnalyticsDBConnection()
    raw_ok, analytics_ok = db.validate_connections()
    
    if raw_ok:
        print("✅ Raw database connection: OK")
    else:
        print("❌ Raw database connection: FAILED")
        return False
    
    if analytics_ok:
        print("✅ Analytics database connection: OK")
    else:
        print("❌ Analytics database connection: FAILED")
        return False
    
    # Test 2: Get current gameweek
    print("\n2️⃣ Testing gameweek detection...")
    current_gw = db.get_current_gameweek()
    if current_gw:
        print(f"✅ Current gameweek detected: {current_gw}")
    else:
        print("❌ Could not detect current gameweek")
        return False
    
    # Test 3: Test analytics operations
    print("\n3️⃣ Testing analytics operations...")
    ops = AnalyticsDBOperations()
    
    analytics_gw = ops.get_current_analytics_gameweek()
    player_count = ops.get_analytics_player_count()
    
    print(f"📊 Analytics gameweek: {analytics_gw}")
    print(f"📊 Analytics player count: {player_count}")
    
    # Test 4: Dual connections for ETL
    print("\n4️⃣ Testing dual connections for ETL...")
    try:
        with db.get_dual_connections() as (raw_conn, analytics_conn):
            # Test reading from raw
            raw_players = raw_conn.execute("SELECT COUNT(*) FROM player_standard").fetchone()[0]
            print(f"✅ Can read from raw DB: {raw_players} players")
            
            # Test reading from analytics  
            analytics_players = analytics_conn.execute("SELECT COUNT(*) FROM analytics_players").fetchone()[0]
            print(f"✅ Can read from analytics DB: {analytics_players} records")
            
    except Exception as e:
        print(f"❌ Dual connections failed: {e}")
        return False
    
    print("\n🎉 Analytics foundation test completed successfully!")
    print("📝 Ready to build ETL pipeline...")
    return True

if __name__ == "__main__":
    success = test_analytics_foundation()
    sys.exit(0 if success else 1)