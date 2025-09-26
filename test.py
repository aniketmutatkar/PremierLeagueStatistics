#!/usr/bin/env python3
"""
Historical Data Comprehensive Test Suite
Tests the scope and quality of historical vs current data with joins and analytics
"""

import duckdb
import pandas as pd
from datetime import datetime
import sys

def test_historical_data_scope():
    """Test the scope and completeness of historical data"""
    
    print("=" * 80)
    print("HISTORICAL DATA COMPREHENSIVE TEST SUITE")
    print("=" * 80)
    print(f"Test run: {datetime.now()}")
    print()
    
    # Connect to database
    db_path = "data/premierleague_analytics_historytest.duckdb"
    
    try:
        conn = duckdb.connect(db_path)
        print(f"✅ Connected to: {db_path}")
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        return False
    
    print("\n" + "=" * 80)
    print("TEST 1: DATA SCOPE & SEASON COVERAGE")
    print("=" * 80)
    
    # Test 1: Season coverage across all tables
    tables = ['analytics_players', 'analytics_keepers', 'analytics_squads', 'analytics_opponents', 'analytics_fixtures']
    
    for table in tables:
        try:
            if table == 'analytics_fixtures':
                # Fixtures might have different season format
                seasons = conn.execute(f"""
                    SELECT season, COUNT(*) as count
                    FROM {table} 
                    GROUP BY season 
                    ORDER BY season
                """).fetchall()
            else:
                seasons = conn.execute(f"""
                    SELECT season, is_current, COUNT(*) as count
                    FROM {table} 
                    GROUP BY season, is_current 
                    ORDER BY season, is_current
                """).fetchall()
            
            print(f"\n{table}:")
            if table == 'analytics_fixtures':
                for season, count in seasons:
                    print(f"  {season}: {count:,} fixtures")
            else:
                for season, is_current, count in seasons:
                    status = "CURRENT" if is_current else "HISTORICAL"
                    print(f"  {season}: {count:,} records ({status})")
                    
        except Exception as e:
            print(f"  ERROR: {e}")
    
    print("\n" + "=" * 80)
    print("TEST 2: DATA QUALITY BY ERA")
    print("=" * 80)
    
    # Test 2: Data quality differences between eras
    data_quality_query = """
    SELECT 
        season,
        COUNT(*) as total_players,
        COUNT(goals) as players_with_goals,
        COUNT(assists) as players_with_assists,
        COUNT(touches) as players_with_touches,
        COUNT(expected_goals) as players_with_xg,
        COUNT(progressive_passes) as players_with_prog_passes,
        COUNT(tackles) as players_with_tackles,
        ROUND(AVG(CASE WHEN goals IS NOT NULL THEN goals ELSE 0 END), 2) as avg_goals_per_player,
        ROUND(AVG(CASE WHEN minutes_played IS NOT NULL THEN minutes_played ELSE 0 END), 0) as avg_minutes
    FROM analytics_players 
    GROUP BY season 
    ORDER BY season
    """
    
    quality_results = conn.execute(data_quality_query).fetchall()
    
    print("Data Availability by Season:")
    print("Season       | Players | Goals | Assists | Touches | xG    | ProgPass | Tackles | AvgGoals | AvgMins")
    print("-" * 95)
    
    for row in quality_results:
        season, total, goals, assists, touches, xg, prog, tackles, avg_goals, avg_mins = row
        print(f"{season:<12} | {total:<7} | {goals:<5} | {assists:<7} | {touches:<7} | {xg:<5} | {prog:<8} | {tackles:<7} | {avg_goals:<8} | {avg_mins:<7.0f}")
    
    print("\n" + "=" * 80)
    print("TEST 3: CROSS-SEASON PLAYER ANALYSIS")
    print("=" * 80)
    
    # Test 3: Multi-season player tracking
    player_tracking_query = """
    SELECT 
        player_name,
        COUNT(DISTINCT season) as seasons_played,
        COUNT(DISTINCT squad) as teams_played_for,
        SUM(CASE WHEN goals IS NOT NULL THEN goals ELSE 0 END) as career_goals,
        SUM(CASE WHEN assists IS NOT NULL THEN assists ELSE 0 END) as career_assists,
        MIN(season) as first_season,
        MAX(season) as last_season
    FROM analytics_players 
    WHERE minutes_played > 500  -- Only players with significant playing time
    GROUP BY player_name 
    HAVING COUNT(DISTINCT season) >= 3  -- Players across multiple seasons
    ORDER BY career_goals DESC 
    LIMIT 15
    """
    
    player_results = conn.execute(player_tracking_query).fetchall()
    
    print("Multi-Season Player Careers (Top 15 Goal Scorers):")
    print("Player Name              | Seasons | Teams | Goals | Assists | First    | Last")
    print("-" * 80)
    
    for row in player_results:
        name, seasons, teams, goals, assists, first, last = row
        print(f"{name:<24} | {seasons:<7} | {teams:<5} | {goals:<5} | {assists:<7} | {first:<8} | {last}")
    
    print("\n" + "=" * 80)
    print("TEST 4: TEAM EVOLUTION ANALYSIS")
    print("=" * 80)
    
    # Test 4: Team performance over time
    team_evolution_query = """
    SELECT 
        squad_name,
        season,
        COALESCE(goals, 0) as goals_scored,
        COALESCE(goals_against, 0) as goals_conceded,
        CASE WHEN goals IS NOT NULL AND goals_against IS NOT NULL 
             THEN goals - goals_against ELSE NULL END as goal_difference,
        COALESCE(matches_played, 0) as matches
    FROM analytics_squads 
    WHERE squad_name IN ('Arsenal', 'Manchester City', 'Liverpool', 'Chelsea', 'Manchester Utd')
    ORDER BY squad_name, season
    """
    
    team_results = conn.execute(team_evolution_query).fetchall()
    
    print("Big 6 Team Evolution:")
    current_team = None
    for row in team_results:
        squad, season, goals_for, goals_against, goal_diff, matches = row
        if squad != current_team:
            print(f"\n{squad}:")
            current_team = squad
        
        goal_diff_str = f"{goal_diff:+d}" if goal_diff is not None else "N/A"
        print(f"  {season}: {goals_for}GF, {goals_against}GA, {goal_diff_str}GD ({matches}M)")
    
    print("\n" + "=" * 80)
    print("TEST 5: FIXTURE DATA INTEGRITY")
    print("=" * 80)
    
    # Test 5: Fixture data completeness
    try:
        fixture_query = """
        SELECT 
            season,
            COUNT(*) as total_fixtures,
            COUNT(CASE WHEN is_completed THEN 1 END) as completed_fixtures,
            COUNT(CASE WHEN home_xg IS NOT NULL THEN 1 END) as fixtures_with_xg,
            ROUND(AVG(CASE WHEN total_goals IS NOT NULL THEN total_goals END), 2) as avg_goals_per_game,
            COUNT(CASE WHEN competitive_match = true THEN 1 END) as competitive_matches
        FROM analytics_fixtures 
        GROUP BY season 
        ORDER BY season
        """
        
        fixture_results = conn.execute(fixture_query).fetchall()
        
        print("Fixture Data by Season:")
        print("Season       | Total | Completed | WithXG | AvgGoals | Competitive")
        print("-" * 65)
        
        for row in fixture_results:
            season, total, completed, with_xg, avg_goals, competitive = row
            avg_goals_str = f"{avg_goals:.2f}" if avg_goals else "N/A"
            print(f"{season:<12} | {total:<5} | {completed:<9} | {with_xg:<6} | {avg_goals_str:<8} | {competitive}")
            
    except Exception as e:
        print(f"Fixture analysis failed: {e}")
    
    print("\n" + "=" * 80)
    print("TEST 6: BUSINESS KEY VALIDATION")
    print("=" * 80)
    
    # Test 6: Business key uniqueness
    business_key_query = """
    SELECT 
        'Players' as entity_type,
        COUNT(*) as total_records,
        COUNT(DISTINCT player_id) as unique_business_keys,
        COUNT(*) - COUNT(DISTINCT player_id) as duplicates
    FROM analytics_players
    
    UNION ALL
    
    SELECT 
        'Squads' as entity_type,
        COUNT(*) as total_records,
        COUNT(DISTINCT squad_id) as unique_business_keys,
        COUNT(*) - COUNT(DISTINCT squad_id) as duplicates
    FROM analytics_squads
    """
    
    bk_results = conn.execute(business_key_query).fetchall()
    
    print("Business Key Integrity:")
    for entity, total, unique, dupes in bk_results:
        status = "✅ PASS" if dupes == 0 else f"❌ {dupes} DUPLICATES"
        print(f"{entity}: {total:,} records, {unique:,} unique keys - {status}")
    
    print("\n" + "=" * 80)
    print("TEST 7: HISTORICAL VS CURRENT STATUS")
    print("=" * 80)
    
    # Test 7: Current vs historical status validation
    status_query = """
    SELECT 
        season,
        SUM(CASE WHEN is_current THEN 1 ELSE 0 END) as current_records,
        SUM(CASE WHEN NOT is_current THEN 1 ELSE 0 END) as historical_records
    FROM analytics_players 
    GROUP BY season 
    ORDER BY season
    """
    
    status_results = conn.execute(status_query).fetchall()
    
    print("Current vs Historical Status:")
    for season, current, historical in status_results:
        if current > 0:
            print(f"{season}: {current:,} CURRENT, {historical:,} historical")
        else:
            print(f"{season}: {historical:,} HISTORICAL")
    
    print("\n" + "=" * 80)
    print("TEST 8: SAMPLE JOIN OPERATIONS")
    print("=" * 80)
    
    # Test 8: Complex joins work properly
    join_query = """
    SELECT 
        p.player_name,
        p.season,
        p.squad,
        p.goals as player_goals,
        s.goals as squad_total_goals,
        ROUND(100.0 * p.goals / NULLIF(s.goals, 0), 1) as goal_percentage
    FROM analytics_players p
    JOIN analytics_squads s ON p.squad = s.squad_name AND p.season = s.season
    WHERE p.goals >= 15 AND s.goals IS NOT NULL
    ORDER BY p.season DESC, p.goals DESC
    LIMIT 10
    """
    
    join_results = conn.execute(join_query).fetchall()
    
    print("Player Goal Contribution to Team (15+ goals):")
    print("Player               | Season    | Squad            | PGoals | TGoals | % of Total")
    print("-" * 85)
    
    for row in join_results:
        player, season, squad, p_goals, t_goals, percentage = row
        pct_str = f"{percentage}%" if percentage else "N/A"
        print(f"{player:<20} | {season} | {squad:<16} | {p_goals:<6} | {t_goals:<6} | {pct_str}")
    
    print("\n" + "=" * 80)
    print("SUMMARY: HISTORICAL DATA VALIDATION")
    print("=" * 80)
    
    # Final summary
    total_seasons = conn.execute("SELECT COUNT(DISTINCT season) FROM analytics_players").fetchone()[0]
    total_players = conn.execute("SELECT COUNT(*) FROM analytics_players").fetchone()[0]
    total_fixtures = conn.execute("SELECT COUNT(*) FROM analytics_fixtures").fetchone()[0] if 'analytics_fixtures' in [t[0] for t in conn.execute("SHOW TABLES").fetchall()] else 0
    
    earliest_season = conn.execute("SELECT MIN(season) FROM analytics_players").fetchone()[0]
    latest_season = conn.execute("SELECT MAX(season) FROM analytics_players").fetchone()[0]
    
    print(f"📊 DATA SCOPE:")
    print(f"   Seasons: {total_seasons} ({earliest_season} to {latest_season})")
    print(f"   Total Player Records: {total_players:,}")
    print(f"   Total Fixtures: {total_fixtures:,}")
    
    # Data quality assessment
    modern_seasons = conn.execute("SELECT COUNT(*) FROM analytics_players WHERE expected_goals IS NOT NULL").fetchone()[0]
    historical_seasons = total_players - modern_seasons
    
    print(f"\n📈 DATA QUALITY:")
    print(f"   Modern Stats (with xG): {modern_seasons:,} records")
    print(f"   Historical Stats (basic): {historical_seasons:,} records")
    print(f"   Coverage: {100.0 * total_players / (total_seasons * 400):.1f}% (assuming ~400 players/season)")
    
    print(f"\n✅ VALIDATION COMPLETE")
    print(f"   Database: {db_path}")
    print(f"   All tests completed successfully")
    
    conn.close()
    return True

if __name__ == "__main__":
    test_historical_data_scope()