"""
Data Integrity Testing Script
Tests season assignment, gameweek logic, and data consistency
"""

import duckdb
import sys
from pathlib import Path

# Database path
DB_PATH = "data/premierleague_analytics.duckdb"

def print_section(title):
    """Print a formatted section header"""
    print("\n" + "="*80)
    print(f"  {title}")
    print("="*80)

def run_query(conn, query, description):
    """Run a query and display results"""
    print(f"\n{description}")
    print("-" * 80)
    try:
        result = conn.execute(query).fetchall()
        if result:
            # Get column names
            columns = [desc[0] for desc in conn.execute(query).description]
            
            # Print header
            header = " | ".join(f"{col:20}" for col in columns)
            print(header)
            print("-" * len(header))
            
            # Print rows
            for row in result[:50]:  # Limit to first 50 rows
                row_str = " | ".join(f"{str(val):20}" for val in row)
                print(row_str)
            
            if len(result) > 50:
                print(f"\n... ({len(result) - 50} more rows)")
            
            print(f"\nTotal rows: {len(result)}")
        else:
            print("No results returned")
    except Exception as e:
        print(f"❌ Error: {e}")
    print()

def test_data_integrity():
    """Run comprehensive data integrity tests"""
    
    # Check if database exists
    if not Path(DB_PATH).exists():
        print(f"❌ Database not found at {DB_PATH}")
        sys.exit(1)
    
    print(f"🔍 Testing Data Integrity for: {DB_PATH}")
    print(f"Current Date: {duckdb.execute('SELECT CURRENT_DATE').fetchone()[0]}")
    
    try:
        conn = duckdb.connect(DB_PATH, read_only=True)
        
        # TEST 1: SEASON VERIFICATION
        print_section("TEST 1: SEASON VERIFICATION")
        
        run_query(conn, """
            SELECT 'analytics_players' as table_name, season, COUNT(*) as records
            FROM analytics_players 
            GROUP BY season
            UNION ALL
            SELECT 'analytics_keepers' as table_name, season, COUNT(*) as records  
            FROM analytics_keepers
            GROUP BY season
            UNION ALL
            SELECT 'analytics_squads' as table_name, season, COUNT(*) as records
            FROM analytics_squads 
            GROUP BY season
            UNION ALL
            SELECT 'analytics_opponents' as table_name, season, COUNT(*) as records
            FROM analytics_opponents
            GROUP BY season
            ORDER BY table_name, season
        """, "Season assignment across all tables:")
        
        run_query(conn, """
            SELECT 
                CURRENT_DATE as today,
                CASE 
                    WHEN EXTRACT(MONTH FROM CURRENT_DATE) >= 8 THEN 
                        CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS VARCHAR) || '-' || CAST((EXTRACT(YEAR FROM CURRENT_DATE) + 1) AS VARCHAR)
                    ELSE 
                        CAST((EXTRACT(YEAR FROM CURRENT_DATE) - 1) AS VARCHAR) || '-' || CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS VARCHAR)
                END as expected_season
        """, "Expected season based on current date:")
        
        run_query(conn, """
            SELECT DISTINCT season as actual_season, COUNT(*) as records
            FROM analytics_players 
            WHERE is_current = true
            GROUP BY season
        """, "Actual season in current data:")
        
        # TEST 2: GAMEWEEK VERIFICATION
        print_section("TEST 2: GAMEWEEK VERIFICATION")
        
        run_query(conn, """
            SELECT 
                player_name,
                squad,
                gameweek,
                matches_played,
                starts,
                minutes_played,
                goals,
                season,
                is_current
            FROM analytics_players 
            WHERE player_name LIKE '%Haaland%' 
            ORDER BY gameweek DESC
        """, "Haaland's data (checking gameweek vs matches played):")
        
        run_query(conn, """
            SELECT 
                player_name,
                squad,
                gameweek,
                matches_played,
                (gameweek - matches_played) as gameweek_mismatch,
                minutes_played,
                is_current
            FROM analytics_players 
            WHERE is_current = true 
                AND ABS(gameweek - matches_played) > 1
            ORDER BY ABS(gameweek - matches_played) DESC
            LIMIT 20
        """, "Players with gameweek mismatch (off by more than 1):")
        
        # TEST 3: SQUAD GAMEWEEK CONSISTENCY
        print_section("TEST 3: SQUAD GAMEWEEK CONSISTENCY")
        
        run_query(conn, """
            SELECT 
                gameweek,
                COUNT(DISTINCT squad_name) as squads_at_this_gameweek,
                COUNT(*) as total_records
            FROM analytics_squads 
            WHERE is_current = true
            GROUP BY gameweek
            ORDER BY gameweek DESC
        """, "Squads by gameweek in current data:")
        
        run_query(conn, """
            SELECT 
                squad_name,
                gameweek,
                matches_played,
                (gameweek - matches_played) as difference,
                wins + draws + losses as calculated_matches
            FROM analytics_squads 
            WHERE is_current = true
            ORDER BY ABS(gameweek - matches_played) DESC
        """, "Squad gameweek vs matches played:")
        
        # TEST 4: OFF-BY-ONE ERROR DETECTION
        print_section("TEST 4: OFF-BY-ONE ERROR DETECTION")
        
        run_query(conn, """
            SELECT 
                squad,
                AVG(gameweek - matches_played) as avg_gameweek_difference,
                COUNT(*) as player_count,
                COUNT(CASE WHEN (gameweek - matches_played) = 1 THEN 1 END) as off_by_one_count,
                ROUND(COUNT(CASE WHEN (gameweek - matches_played) = 1 THEN 1 END)::DECIMAL / COUNT(*) * 100, 1) as off_by_one_percentage
            FROM analytics_players 
            WHERE is_current = true AND matches_played > 0
            GROUP BY squad
            ORDER BY avg_gameweek_difference DESC
        """, "Off-by-one analysis by squad:")
        
        run_query(conn, """
            SELECT 
                (gameweek - matches_played) as difference,
                COUNT(*) as player_count,
                ROUND(COUNT(*)::DECIMAL / (SELECT COUNT(*) FROM analytics_players WHERE is_current = true AND matches_played > 0) * 100, 1) as percentage
            FROM analytics_players 
            WHERE is_current = true AND matches_played > 0
            GROUP BY (gameweek - matches_played)
            ORDER BY difference
        """, "Distribution of gameweek - matches_played difference:")
        
        # TEST 5: SEASON FORMAT CHECK
        print_section("TEST 5: SEASON FORMAT CHECK")
        
        run_query(conn, """
            SELECT 
                season,
                LENGTH(season) as season_length,
                SUBSTRING(season, 1, 4) as start_year,
                CASE 
                    WHEN LENGTH(season) >= 8 THEN SUBSTRING(season, 6, 4)
                    ELSE SUBSTRING(season, 6, 2)
                END as end_year,
                COUNT(*) as record_count,
                CASE 
                    WHEN season LIKE '2025-2026' THEN '❌ WRONG: Future season'
                    WHEN season LIKE '2024-2025' THEN '✅ CORRECT: Current season'
                    WHEN season LIKE '2024-25' THEN '✅ CORRECT: Current season (short format)'
                    ELSE '⚠️  UNKNOWN FORMAT'
                END as season_assessment
            FROM analytics_players 
            GROUP BY season
            ORDER BY season
        """, "Season format analysis:")
        
        # TEST 6: CURRENT FLAGS CHECK
        print_section("TEST 6: CURRENT FLAGS CHECK")
        
        run_query(conn, """
            SELECT 
                is_current,
                COUNT(*) as record_count,
                COUNT(DISTINCT player_name) as unique_players,
                MAX(gameweek) as max_gameweek,
                MIN(gameweek) as min_gameweek
            FROM analytics_players 
            GROUP BY is_current
        """, "Current flag distribution (players):")
        
        run_query(conn, """
            SELECT 
                is_current,
                COUNT(*) as record_count,
                COUNT(DISTINCT squad_name) as unique_squads,
                MAX(gameweek) as max_gameweek,
                MIN(gameweek) as min_gameweek
            FROM analytics_squads 
            GROUP BY is_current
        """, "Current flag distribution (squads):")
        
        # TEST 7: DATA FRESHNESS
        print_section("TEST 7: DATA FRESHNESS")
        
        run_query(conn, """
            SELECT 'analytics_players' as table_name, MAX(updated_at) as last_update, COUNT(*) as total_records
            FROM analytics_players
            UNION ALL
            SELECT 'analytics_keepers' as table_name, MAX(updated_at) as last_update, COUNT(*) as total_records 
            FROM analytics_keepers
            UNION ALL
            SELECT 'analytics_squads' as table_name, MAX(updated_at) as last_update, COUNT(*) as total_records
            FROM analytics_squads
            UNION ALL
            SELECT 'analytics_opponents' as table_name, MAX(updated_at) as last_update, COUNT(*) as total_records
            FROM analytics_opponents
        """, "Last update timestamp for each table:")
        
        # TEST 8: OVERALL INTEGRITY SUMMARY
        print_section("TEST 8: OVERALL INTEGRITY SUMMARY")
        
        # Check season format
        season_check = conn.execute("""
            SELECT 
                CASE WHEN season LIKE '2024-25' OR season LIKE '2024-2025' THEN '✅ PASS' ELSE '❌ FAIL' END as status,
                season as details
            FROM analytics_players 
            WHERE is_current = true 
            LIMIT 1
        """).fetchone()
        
        # Check gameweek consistency
        gameweek_check = conn.execute("""
            SELECT 
                CASE WHEN AVG(ABS(gameweek - matches_played)) < 0.5 THEN '✅ PASS' ELSE '❌ FAIL' END as status,
                'Avg difference: ' || ROUND(AVG(ABS(gameweek - matches_played)), 2) as details
            FROM analytics_players 
            WHERE is_current = true AND matches_played > 0
        """).fetchone()
        
        # Check current data exists
        current_check = conn.execute("""
            SELECT 
                CASE WHEN COUNT(*) > 0 THEN '✅ PASS' ELSE '❌ FAIL' END as status,
                CAST(COUNT(*) AS VARCHAR) || ' current records' as details
            FROM analytics_players 
            WHERE is_current = true
        """).fetchone()
        
        print("\nIntegrity Check Summary:")
        print("-" * 80)
        print(f"Season Format:           {season_check[0]:15} | {season_check[1]}")
        print(f"Gameweek Consistency:    {gameweek_check[0]:15} | {gameweek_check[1]}")
        print(f"Current Data Flags:      {current_check[0]:15} | {current_check[1]}")
        
        conn.close()
        
        print_section("TESTS COMPLETE")
        print("\n💡 Review the results above to identify data integrity issues.")
        print("Look for:")
        print("  - Season showing as 2025-2026 instead of 2024-25")
        print("  - Gameweek values that don't match matches_played")
        print("  - Systematic off-by-one errors across all players/squads")
        
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    test_data_integrity()