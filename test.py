#!/usr/bin/env python3
"""
Data Quality Tests - Validate consolidated tables work correctly
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from src.database import DatabaseConnection

def run_data_quality_tests():
    """Run comprehensive tests on consolidated data"""
    print("=== DATA QUALITY VALIDATION TESTS ===")
    
    db_connection = DatabaseConnection()
    
    with db_connection.get_connection() as conn:
        print("\n1. TABLE STRUCTURE VALIDATION")
        test_table_structure(conn)
        
        print("\n2. DATA COMPLETENESS TESTS")
        test_data_completeness(conn)
        
        print("\n3. DATA CONSISTENCY TESTS")
        test_data_consistency(conn)
        
        print("\n4. BUSINESS LOGIC TESTS")
        test_business_logic(conn)
        
        print("\n5. SAMPLE DATA INSPECTION")
        inspect_sample_data(conn)

def test_table_structure(conn):
    """Test that all expected tables exist with reasonable structure"""
    expected_tables = [
        'current_player_stats',
        'current_squad_stats', 
        'current_opponent_stats',
        'current_fixtures'
    ]
    
    for table_name in expected_tables:
        try:
            # Check table exists and has data
            count_result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            row_count = count_result[0]
            
            # Get column info
            columns_result = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
            column_count = len(columns_result)
            
            print(f"  ✅ {table_name}: {row_count} rows, {column_count} columns")
            
            # Check for essential columns
            column_names = [col[1] for col in columns_result]
            if table_name == 'current_player_stats':
                essential_cols = ['Player', 'Squad', 'current_through_gameweek']
                missing = [col for col in essential_cols if col not in column_names]
                if missing:
                    print(f"    ⚠️  Missing essential columns: {missing}")
                    
        except Exception as e:
            print(f"  ❌ {table_name}: ERROR - {e}")

def test_data_completeness(conn):
    """Test for missing or null data in critical fields"""
    
    # Player stats completeness
    try:
        # Check for players without names
        null_names = conn.execute("SELECT COUNT(*) FROM current_player_stats WHERE Player IS NULL OR Player = ''").fetchone()[0]
        print(f"  Players with null/empty names: {null_names}")
        
        # Check for players without squads
        null_squads = conn.execute("SELECT COUNT(*) FROM current_player_stats WHERE Squad IS NULL OR Squad = ''").fetchone()[0]
        print(f"  Players with null/empty squads: {null_squads}")
        
        # Check gameweek consistency
        gameweeks = conn.execute("SELECT DISTINCT current_through_gameweek FROM current_player_stats").fetchall()
        print(f"  Distinct gameweeks in player data: {[gw[0] for gw in gameweeks]}")
        
    except Exception as e:
        print(f"  ❌ Player completeness test failed: {e}")
    
    # Squad stats completeness
    try:
        squad_count = conn.execute("SELECT COUNT(*) FROM current_squad_stats").fetchone()[0]
        distinct_squads = conn.execute("SELECT COUNT(DISTINCT Squad) FROM current_squad_stats").fetchone()[0]
        print(f"  Squad stats: {squad_count} rows, {distinct_squads} distinct squads")
        
        if squad_count != 20 or distinct_squads != 20:
            print(f"    ⚠️  Expected 20 Premier League teams, got {distinct_squads}")
            
    except Exception as e:
        print(f"  ❌ Squad completeness test failed: {e}")
    
    # Fixtures completeness
    try:
        total_fixtures = conn.execute("SELECT COUNT(*) FROM current_fixtures").fetchone()[0]
        completed_fixtures = conn.execute("SELECT COUNT(*) FROM current_fixtures WHERE is_completed = true").fetchone()[0]
        print(f"  Fixtures: {total_fixtures} total, {completed_fixtures} completed")
        
        if total_fixtures != 380:
            print(f"    ⚠️  Expected 380 Premier League fixtures, got {total_fixtures}")
            
    except Exception as e:
        print(f"  ❌ Fixtures completeness test failed: {e}")

def test_data_consistency(conn):
    """Test for data consistency across tables"""
    
    try:
        # Check if player squads match fixture teams
        player_squads = set(row[0] for row in conn.execute("SELECT DISTINCT Squad FROM current_player_stats").fetchall())
        fixture_teams = set(row[0] for row in conn.execute("SELECT DISTINCT home_team FROM current_fixtures UNION SELECT DISTINCT away_team FROM current_fixtures").fetchall())
        
        squad_only = player_squads - fixture_teams
        fixture_only = fixture_teams - player_squads
        
        print(f"  Teams in player data: {len(player_squads)}")
        print(f"  Teams in fixture data: {len(fixture_teams)}")
        
        if squad_only:
            print(f"    ⚠️  Teams in player data but not fixtures: {squad_only}")
        if fixture_only:
            print(f"    ⚠️  Teams in fixtures but not player data: {fixture_only}")
        
        # Check squad vs opponent data consistency
        squad_teams = set(row[0] for row in conn.execute("SELECT DISTINCT Squad FROM current_squad_stats").fetchall())
        opponent_teams = set(row[0].replace('vs ', '') for row in conn.execute("SELECT DISTINCT Squad FROM current_opponent_stats").fetchall() if row[0].startswith('vs '))
        
        if len(squad_teams ^ opponent_teams) > 0:  # symmetric difference
            print(f"    ⚠️  Mismatch between squad and opponent team lists")
        else:
            print(f"  ✅ Squad and opponent data have consistent team lists")
            
    except Exception as e:
        print(f"  ❌ Consistency test failed: {e}")

def test_business_logic(conn):
    """Test business logic and reasonable data ranges"""
    
    try:
        # Check for reasonable goal totals
        high_scorers = conn.execute("SELECT Player, Squad, PerformanceGls FROM current_player_stats WHERE PerformanceGls > 10 ORDER BY PerformanceGls DESC LIMIT 5").fetchall()
        if high_scorers:
            print(f"  Top scorers (should be reasonable):")
            for player, squad, goals in high_scorers:
                print(f"    {player} ({squad}): {goals} goals")
        
        # Check minutes played are reasonable
        high_minutes = conn.execute("SELECT Player, Squad, PlayingTimeMin FROM current_player_stats WHERE PlayingTimeMin > 2000 ORDER BY PlayingTimeMin DESC LIMIT 3").fetchall()
        if high_minutes:
            print(f"  High minutes played (check reasonableness):")
            for player, squad, minutes in high_minutes:
                print(f"    {player} ({squad}): {minutes} minutes")
        
        # Check for negative stats (should be rare/none)
        negative_goals = conn.execute("SELECT COUNT(*) FROM current_player_stats WHERE PerformanceGls < 0").fetchone()[0]
        if negative_goals > 0:
            print(f"    ⚠️  Found {negative_goals} players with negative goals")
        else:
            print(f"  ✅ No negative goal values found")
            
    except Exception as e:
        print(f"  ❌ Business logic test failed: {e}")

def inspect_sample_data(conn):
    """Show sample data for manual inspection"""
    
    print("\n  Sample Player Data (Top 5 by Goals):")
    try:
        sample_players = conn.execute("""
            SELECT Player, Squad, PerformanceGls, PerformanceAst, PlayingTimeMin, TotalCmp 
            FROM current_player_stats 
            WHERE PerformanceGls > 0 
            ORDER BY PerformanceGls DESC 
            LIMIT 5
        """).fetchall()
        
        for player, squad, goals, assists, minutes, passes in sample_players:
            print(f"    {player} ({squad}): {goals}G {assists}A, {minutes}min, {passes} passes")
            
    except Exception as e:
        print(f"    ❌ Could not retrieve player sample: {e}")
    
    print("\n  Sample Squad Data (Top 3 by Goals):")
    try:
        sample_squads = conn.execute("""
            SELECT Squad, PerformanceGls, PerformanceAst
            FROM current_squad_stats 
            ORDER BY PerformanceGls DESC 
            LIMIT 3
        """).fetchall()
        
        for squad, goals, assists in sample_squads:
            print(f"    {squad}: {goals}G {assists}A")
            
    except Exception as e:
        print(f"    ❌ Could not retrieve squad sample: {e}")
    
    print("\n  Current Gameweek Fixtures Status:")
    try:
        current_gw = conn.execute("SELECT MAX(gameweek) FROM current_fixtures WHERE is_completed = false").fetchone()[0]
        gw_fixtures = conn.execute(f"""
            SELECT home_team, away_team, is_completed, home_score, away_score 
            FROM current_fixtures 
            WHERE gameweek = {current_gw} 
            LIMIT 5
        """).fetchall()
        
        for home, away, completed, home_score, away_score in gw_fixtures:
            if completed:
                print(f"    ✅ {home} {home_score}-{away_score} {away}")
            else:
                print(f"    ⏳ {home} vs {away}")
                
    except Exception as e:
        print(f"    ❌ Could not retrieve fixture sample: {e}")

def run_simple_analysis_queries(conn):
    """Run some basic analysis queries to test functionality"""
    
    print("\n=== SIMPLE ANALYSIS QUERIES ===")
    
    # Top scorers
    print("\nTop 5 Premier League Scorers:")
    try:
        top_scorers = conn.execute("""
            SELECT Player, Squad, PerformanceGls as Goals
            FROM current_player_stats 
            ORDER BY PerformanceGls DESC 
            LIMIT 5
        """).fetchall()
        
        for i, (player, squad, goals) in enumerate(top_scorers, 1):
            print(f"  {i}. {player} ({squad}) - {goals} goals")
    except Exception as e:
        print(f"  ❌ Error: {e}")
    
    # Team with most goals
    print("\nTeams by Goals Scored:")
    try:
        team_goals = conn.execute("""
            SELECT Squad, PerformanceGls as Goals
            FROM current_squad_stats 
            ORDER BY PerformanceGls DESC 
            LIMIT 5
        """).fetchall()
        
        for i, (squad, goals) in enumerate(team_goals, 1):
            print(f"  {i}. {squad} - {goals} goals")
    except Exception as e:
        print(f"  ❌ Error: {e}")

if __name__ == "__main__":
    run_data_quality_tests()
    
    db_connection = DatabaseConnection()
    with db_connection.get_connection() as conn:
        run_simple_analysis_queries(conn)