#!/usr/bin/env python3
"""
Historical Data Validation Suite v2
Validates 15+ seasons of Premier League historical data (2010-2025)
"""

import duckdb
import pandas as pd
from datetime import datetime

def validate_historical_data():
    """Comprehensive validation of historical Premier League data"""
    
    print("HISTORICAL DATA VALIDATION SUITE v2")
    print("=" * 60)
    print(f"Run time: {datetime.now()}")
    
    # Connect to production database
    db_path = "data/premierleague_analytics.duckdb"
    
    try:
        conn = duckdb.connect(db_path)
        print(f"Connected to: {db_path}")
    except Exception as e:
        print(f"FAILED to connect: {e}")
        return False
    
    print("\n" + "=" * 60)
    print("1. SEASON COVERAGE ANALYSIS")
    print("=" * 60)
    
    # Season coverage across all entity types
    season_coverage = conn.execute("""
        SELECT 
            season,
            COUNT(DISTINCT player_name) as unique_players,
            COUNT(*) as total_player_records,
            MAX(gameweek) as max_gameweek,
            COUNT(CASE WHEN is_current THEN 1 END) as current_records
        FROM analytics_players 
        GROUP BY season 
        ORDER BY season
    """).fetchall()
    
    print("Season Coverage (Player Data):")
    print("Season    | Players | Records | MaxGW | Current")
    print("-" * 50)
    
    total_seasons = 0
    total_players = 0
    oldest_season = None
    newest_season = None
    
    for season, players, records, max_gw, current in season_coverage:
        total_seasons += 1
        total_players += records
        if oldest_season is None:
            oldest_season = season
        newest_season = season
        
        print(f"{season} | {players:>7} | {records:>7} | {max_gw:>5} | {current:>7}")
    
    print(f"\nSUMMARY: {total_seasons} seasons, {total_players:,} total records")
    print(f"SPAN: {oldest_season} to {newest_season}")
    
    print("\n" + "=" * 60)
    print("2. FIXTURE DATA COMPLETENESS")
    print("=" * 60)
    
    # Fixture completeness
    fixture_data = conn.execute("""
        SELECT 
            season,
            COUNT(*) as total_fixtures,
            COUNT(CASE WHEN is_completed THEN 1 END) as completed,
            COUNT(CASE WHEN home_xg IS NOT NULL THEN 1 END) as with_xg,
            ROUND(AVG(CASE WHEN total_goals IS NOT NULL THEN total_goals END), 2) as avg_goals
        FROM analytics_fixtures 
        GROUP BY season 
        ORDER BY season
    """).fetchall()
    
    print("Fixture Completeness:")
    print("Season    | Total | Completed | WithXG | AvgGoals")
    print("-" * 50)
    
    xg_era_start = None
    for season, total, completed, with_xg, avg_goals in fixture_data:
        avg_str = f"{avg_goals:.2f}" if avg_goals else "N/A"
        print(f"{season} | {total:>5} | {completed:>9} | {with_xg:>6} | {avg_str:>8}")
        
        if with_xg > 0 and xg_era_start is None:
            xg_era_start = season
    
    if xg_era_start:
        print(f"\nxG data starts from: {xg_era_start}")
    
    print("\n" + "=" * 60)
    print("3. DATA QUALITY BY ERA")
    print("=" * 60)
    
    # Data quality analysis
    quality_analysis = conn.execute("""
        SELECT 
            season,
            COUNT(*) as players,
            ROUND(100.0 * COUNT(goals) / COUNT(*), 1) as pct_goals,
            ROUND(100.0 * COUNT(touches) / COUNT(*), 1) as pct_touches,
            ROUND(100.0 * COUNT(expected_goals) / COUNT(*), 1) as pct_xg,
            ROUND(100.0 * COUNT(progressive_passes) / COUNT(*), 1) as pct_prog_pass
        FROM analytics_players 
        GROUP BY season 
        ORDER BY season
    """).fetchall()
    
    print("Data Availability (% of players with stat):")
    print("Season    | Players | Goals | Touches | xG    | ProgPass")
    print("-" * 60)
    
    for season, players, pct_goals, pct_touches, pct_xg, pct_prog in quality_analysis:
        print(f"{season} | {players:>7} | {pct_goals:>5}% | {pct_touches:>7}% | {pct_xg:>5}% | {pct_prog:>8}%")
    
    print("\n" + "=" * 60)
    print("4. HISTORICAL PLAYER CAREERS")
    print("=" * 60)
    
    # Multi-season player analysis
    career_analysis = conn.execute("""
        SELECT 
            player_name,
            COUNT(DISTINCT season) as seasons_active,
            COUNT(DISTINCT squad) as teams,
            SUM(CASE WHEN goals IS NOT NULL THEN goals ELSE 0 END) as career_goals,
            MIN(season) as debut_season,
            MAX(season) as last_season
        FROM analytics_players 
        WHERE minutes_played > 500  -- Significant playing time
        GROUP BY player_name 
        HAVING COUNT(DISTINCT season) >= 5  -- Multi-season careers
        ORDER BY career_goals DESC 
        LIMIT 15
    """).fetchall()
    
    print("Long-Term Premier League Careers (5+ seasons):")
    print("Player                | Seasons | Teams | Goals | Debut    | Last")
    print("-" * 70)
    
    for name, seasons, teams, goals, debut, last in career_analysis:
        print(f"{name:<20} | {seasons:>7} | {teams:>5} | {goals:>5} | {debut} | {last}")
    
    print("\n" + "=" * 60)
    print("5. CLUB EVOLUTION ANALYSIS")
    print("=" * 60)
    
    # Big 6 evolution
    big6_evolution = conn.execute("""
        SELECT 
            squad_name,
            season,
            COALESCE(goals, 0) as goals_for,
            COALESCE(goals_against, 0) as goals_against,
            CASE WHEN goals IS NOT NULL AND goals_against IS NOT NULL 
                 THEN goals - goals_against ELSE NULL END as goal_diff
        FROM analytics_squads 
        WHERE squad_name IN ('Arsenal', 'Chelsea', 'Liverpool', 'Manchester City', 'Manchester Utd', 'Tottenham')
        ORDER BY squad_name, season
    """).fetchall()
    
    print("Big 6 Goal Difference Evolution (sample):")
    current_team = None
    team_count = 0
    
    for squad, season, gf, ga, gd in big6_evolution:
        if squad != current_team:
            if team_count >= 2:  # Only show first 2 teams to save space
                break
            print(f"\n{squad}:")
            current_team = squad
            team_count += 1
        
        gd_str = f"{gd:+d}" if gd is not None else "N/A"
        print(f"  {season}: {gf}GF {ga}GA {gd_str}GD")
    
    print("\n" + "=" * 60)
    print("6. BUSINESS KEY INTEGRITY")
    print("=" * 60)
    
    # Check for actual business key problems
    player_key_check = conn.execute("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT player_id) as unique_player_ids,
            COUNT(DISTINCT CONCAT(player_id, '_', gameweek)) as unique_player_gameweeks
        FROM analytics_players
    """).fetchone()
    
    squad_key_check = conn.execute("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT squad_id) as unique_squad_ids,
            COUNT(DISTINCT CONCAT(squad_id, '_', gameweek)) as unique_squad_gameweeks
        FROM analytics_squads
    """).fetchone()
    
    player_total, player_unique_ids, player_unique_gw = player_key_check
    squad_total, squad_unique_ids, squad_unique_gw = squad_key_check
    
    print("Business Key Analysis:")
    print(f"Players: {player_total:,} records, {player_unique_ids:,} unique IDs, {player_unique_gw:,} unique ID+GW combinations")
    print(f"Squads:  {squad_total:,} records, {squad_unique_ids:,} unique IDs, {squad_unique_gw:,} unique ID+GW combinations")
    
    # Legitimate duplicates (same business key, different gameweeks)
    if player_total == player_unique_gw:
        print("✓ No unexpected duplicates - each player+gameweek is unique")
    else:
        duplicates = player_total - player_unique_gw
        print(f"⚠ {duplicates:,} potential duplicate player+gameweek combinations")
    
    print("\n" + "=" * 60)
    print("7. CROSS-TABLE JOIN VALIDATION")
    print("=" * 60)
    
    # Test joins between tables
    join_test = conn.execute("""
        SELECT 
            p.season,
            COUNT(DISTINCT p.player_name) as players_with_squad_data,
            COUNT(DISTINCT s.squad_name) as squads_with_player_data
        FROM analytics_players p
        LEFT JOIN analytics_squads s ON p.squad = s.squad_name AND p.season = s.season
        WHERE p.season IN (
            SELECT season FROM analytics_players GROUP BY season ORDER BY season LIMIT 3
        )
        GROUP BY p.season
        ORDER BY p.season
    """).fetchall()
    
    print("Player-Squad Join Test (first 3 seasons):")
    for season, players, squads in join_test:
        print(f"{season}: {players} players linked to squad data, {squads} squads with players")
    
    print("\n" + "=" * 60)
    print("8. CURRENT VS HISTORICAL STATUS")
    print("=" * 60)
    
    # Current status distribution
    status_summary = conn.execute("""
        SELECT 
            is_current,
            COUNT(*) as record_count,
            COUNT(DISTINCT season) as season_count,
            MIN(season) as earliest_season,
            MAX(season) as latest_season
        FROM analytics_players 
        GROUP BY is_current
    """).fetchall()
    
    print("Record Status Distribution:")
    for is_current, count, seasons, earliest, latest in status_summary:
        status = "CURRENT" if is_current else "HISTORICAL"
        print(f"{status}: {count:,} records across {seasons} seasons ({earliest} to {latest})")
    
    print("\n" + "=" * 60)
    print("VALIDATION SUMMARY")
    print("=" * 60)
    
    # Final summary
    total_fixtures = conn.execute("SELECT COUNT(*) FROM analytics_fixtures").fetchone()[0]
    
    print(f"DATABASE SCOPE:")
    print(f"  Seasons: {total_seasons} ({oldest_season} to {newest_season})")
    print(f"  Player records: {total_players:,}")
    print(f"  Fixtures: {total_fixtures:,}")
    print(f"  Time span: {2025 - int(oldest_season.split('-')[0])} years of data")
    
    # Data era classification
    if xg_era_start:
        historical_seasons = int(xg_era_start.split('-')[0]) - int(oldest_season.split('-')[0])
        modern_seasons = total_seasons - historical_seasons
        print(f"\nDATA ERAS:")
        print(f"  Historical era: {historical_seasons} seasons (basic stats)")
        print(f"  Modern era: {modern_seasons} seasons (full stats including xG)")
    
    print(f"\n✓ VALIDATION COMPLETE")
    print(f"Historical data successfully spans {total_seasons} seasons")
    
    conn.close()
    return True

if __name__ == "__main__":
    validate_historical_data()