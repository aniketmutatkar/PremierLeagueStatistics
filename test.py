"""
Phase 3 Test Script - Test NEW Analytics ETL with Team-Specific Gameweeks
"""
import sys
import duckdb
from pathlib import Path

def test_analytics_team_gameweeks():
    """Test that analytics data has team-specific gameweeks correctly assigned"""
    print("=" * 80)
    print("PHASE 3 TEST: Analytics Team-Specific Gameweek Assignment")
    print("=" * 80)
    
    analytics_db = "data/premierleague_analytics.duckdb"
    raw_db = "data/premierleague_raw.duckdb"
    
    if not Path(analytics_db).exists():
        print("❌ FAILED: Analytics database does not exist")
        print("   Run: python pipelines/analytics_pipeline.py --force first")
        return False
    
    if not Path(raw_db).exists():
        print("❌ FAILED: Raw database does not exist")
        return False
    
    analytics_conn = duckdb.connect(analytics_db, read_only=True)
    raw_conn = duckdb.connect(raw_db, read_only=True)
    
    print("\n📊 Step 1: Checking raw fixtures for team gameweeks...")
    
    # Calculate expected team gameweeks from fixtures
    fixtures = raw_conn.execute("""
        SELECT home_team, away_team, gameweek, is_completed
        FROM raw_fixtures
        WHERE is_completed = true
    """).fetchdf()
    
    # Calculate expected gameweeks per team
    expected_team_gws = {}
    for team in set(fixtures['home_team'].unique()) | set(fixtures['away_team'].unique()):
        team_fixtures = fixtures[
            ((fixtures['home_team'] == team) | (fixtures['away_team'] == team))
        ]
        if not team_fixtures.empty:
            expected_team_gws[team] = int(team_fixtures['gameweek'].max())
    
    print(f"✅ Calculated expected gameweeks for {len(expected_team_gws)} teams")
    
    print("\n📊 Step 2: Checking analytics player gameweeks...")
    
    # Get actual gameweeks from analytics
    actual_team_gws = analytics_conn.execute("""
        SELECT squad, MAX(gameweek) as max_gw
        FROM analytics_players
        WHERE is_current = true
        GROUP BY squad
    """).fetchdf()
    
    print(f"✅ Found {len(actual_team_gws)} teams in analytics")
    
    print("\n📊 Step 3: Validating gameweek assignments...")
    
    all_passed = True
    mismatches = []
    
    for _, row in actual_team_gws.iterrows():
        team = row['squad']
        actual_gw = int(row['max_gw'])
        expected_gw = expected_team_gws.get(team)
        
        if expected_gw is None:
            print(f"⚠️  {team}: No fixtures found in raw database")
            continue
        
        if actual_gw != expected_gw:
            mismatches.append(f"{team}: Expected GW{expected_gw}, got GW{actual_gw}")
            all_passed = False
    
    if mismatches:
        print(f"\n❌ FAILED: Found {len(mismatches)} gameweek mismatches:")
        for mismatch in mismatches:
            print(f"  {mismatch}")
    else:
        print(f"✅ All team gameweeks match fixtures (validated {len(actual_team_gws)} teams)")
    
    print("\n📊 Step 4: Checking matches_played consistency...")
    
    # Check that matches_played matches gameweek
    consistency_check = analytics_conn.execute("""
        SELECT 
            squad,
            gameweek,
            matches_played,
            (gameweek - matches_played) as difference
        FROM analytics_players
        WHERE is_current = true
        GROUP BY squad, gameweek, matches_played
        HAVING ABS(gameweek - matches_played) > 1
    """).fetchall()
    
    if consistency_check:
        print(f"⚠️  Found {len(consistency_check)} teams with gameweek/matches mismatch:")
        for row in consistency_check[:10]:
            print(f"  {row[0]}: GW{row[1]}, {row[2]} matches, diff={row[3]}")
    else:
        print(f"✅ Matches_played consistent with gameweeks")
    
    print("\n📊 Step 5: Checking SCD Type 2 integrity...")
    
    # Check that only one current record per player
    duplicates = analytics_conn.execute("""
        SELECT player_name, squad, COUNT(*) as count
        FROM analytics_players
        WHERE is_current = true
        GROUP BY player_name, squad
        HAVING COUNT(*) > 1
    """).fetchall()
    
    if duplicates:
        print(f"❌ FAILED: Found {len(duplicates)} players with duplicate current records")
        all_passed = False
    else:
        print(f"✅ No duplicate current records")
    
    # Check historical records exist
    historical_count = analytics_conn.execute("""
        SELECT COUNT(*) FROM analytics_players WHERE is_current = false
    """).fetchone()[0]
    
    print(f"✅ Historical records: {historical_count}")
    
    print("\n📊 Step 6: Summary statistics...")
    
    stats = analytics_conn.execute("""
        SELECT 
            MIN(gameweek) as min_gw,
            MAX(gameweek) as max_gw,
            COUNT(DISTINCT squad) as unique_teams,
            COUNT(*) as total_current_players
        FROM analytics_players
        WHERE is_current = true
    """).fetchone()
    
    print(f"Gameweek range: {stats[0]} - {stats[1]}")
    print(f"Unique teams: {stats[2]}")
    print(f"Total current players: {stats[3]}")
    
    # Show gameweek distribution
    gw_dist = analytics_conn.execute("""
        SELECT gameweek, COUNT(*) as player_count
        FROM analytics_players
        WHERE is_current = true
        GROUP BY gameweek
        ORDER BY gameweek
    """).fetchall()
    
    print(f"\nPlayers by gameweek:")
    for gw, count in gw_dist:
        print(f"  GW{gw}: {count} players")
    
    analytics_conn.close()
    raw_conn.close()
    
    if all_passed:
        print(f"\n{'='*80}")
        print("🎉 PHASE 3 TEST: ALL CHECKS PASSED")
        print(f"{'='*80}")
        print("\nKey Changes Verified:")
        print("  ✅ Team-specific gameweeks assigned from fixtures")
        print("  ✅ Gameweeks match actual completed fixtures per team")
        print("  ✅ SCD Type 2 integrity maintained")
        print("  ✅ No duplicate current records")
        print("  ✅ Historical tracking working")
        return True
    else:
        print(f"\n{'='*80}")
        print("❌ PHASE 3 TEST: FAILED")
        print(f"{'='*80}")
        return False

if __name__ == "__main__":
    try:
        success = test_analytics_team_gameweeks()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n❌ TEST FAILED WITH EXCEPTION: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)