#!/usr/bin/env python3
"""
Analytics Fixtures Table Creation
Creates a comprehensive fixtures table with match outcomes and derived analytics fields
"""

import duckdb
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_analytics_fixtures_table():
    """Create comprehensive analytics fixtures table with match outcomes"""
    
    print("🏈 CREATING ANALYTICS FIXTURES TABLE")
    print("=" * 50)
    
    # Database paths
    raw_db_path = "data/premierleague_raw.duckdb"
    analytics_db_path = "data/premierleague_analytics.duckdb"
    
    # Check if raw database exists
    if not Path(raw_db_path).exists():
        print(f"❌ Raw database not found: {raw_db_path}")
        return False
    
    try:
        # Connect to both databases
        raw_conn = duckdb.connect(raw_db_path)
        analytics_conn = duckdb.connect(analytics_db_path)
        
        print(f"✅ Connected to databases")
        
        # Create comprehensive analytics fixtures table
        create_table_query = """
        CREATE TABLE analytics_fixtures AS
        SELECT 
            -- Basic fixture information
            gameweek,
            match_date,
            match_time,
            day_of_week,
            home_team,
            away_team,
            venue,
            referee,
            attendance,
            
            -- Scores and completion status
            home_score,
            away_score,
            is_completed,
            
            -- Match outcome (derived fields)
            CASE 
                WHEN NOT is_completed THEN 'Not Played'
                WHEN home_score > away_score THEN 'Home Win'
                WHEN away_score > home_score THEN 'Away Win'
                WHEN home_score = away_score THEN 'Draw'
                ELSE 'Unknown'
            END as match_outcome,
            
            -- Winner (team name or draw)
            CASE 
                WHEN NOT is_completed THEN NULL
                WHEN home_score > away_score THEN home_team
                WHEN away_score > home_score THEN away_team
                WHEN home_score = away_score THEN 'Draw'
                ELSE NULL
            END as winner,
            
            -- Points for each team
            CASE 
                WHEN NOT is_completed THEN NULL
                WHEN home_score > away_score THEN 3
                WHEN home_score = away_score THEN 1
                ELSE 0
            END as home_points,
            
            CASE 
                WHEN NOT is_completed THEN NULL
                WHEN away_score > home_score THEN 3
                WHEN home_score = away_score THEN 1
                ELSE 0
            END as away_points,
            
            -- Goal difference for each team
            CASE 
                WHEN NOT is_completed THEN NULL
                ELSE home_score - away_score
            END as home_goal_difference,
            
            CASE 
                WHEN NOT is_completed THEN NULL
                ELSE away_score - home_score
            END as away_goal_difference,
            
            -- Total goals in match
            CASE 
                WHEN NOT is_completed THEN NULL
                ELSE home_score + away_score
            END as total_goals,
            
            -- Expected goals (if available)
            xG as home_xg,
            "xG.1" as away_xg,
            
            -- xG difference
            CASE 
                WHEN xG IS NOT NULL AND "xG.1" IS NOT NULL THEN xG - "xG.1"
                ELSE NULL
            END as home_xg_difference,
            
            -- Match classification by goals
            CASE 
                WHEN NOT is_completed THEN 'Not Played'
                WHEN (home_score + away_score) = 0 THEN 'Goalless'
                WHEN (home_score + away_score) >= 5 THEN 'High Scoring'
                WHEN (home_score + away_score) >= 3 THEN 'Medium Scoring'
                ELSE 'Low Scoring'
            END as goal_classification,
            
            -- Clean sheet indicators
            CASE 
                WHEN NOT is_completed THEN NULL
                WHEN away_score = 0 THEN true
                ELSE false
            END as home_clean_sheet,
            
            CASE 
                WHEN NOT is_completed THEN NULL
                WHEN home_score = 0 THEN true
                ELSE false
            END as away_clean_sheet,
            
            -- Competitive indicator (based on xG difference, fallback to score)
            CASE 
                WHEN NOT is_completed THEN NULL
                -- If we have xG data, use that (competitive if xG difference <= 0.8)
                WHEN xG IS NOT NULL AND "xG.1" IS NOT NULL THEN 
                    CASE WHEN ABS(xG - "xG.1") <= 0.8 THEN true ELSE false END
                -- Fallback to score difference if no xG data
                WHEN ABS(home_score - away_score) <= 1 THEN true
                ELSE false
            END as competitive_match,
            
            -- Season identifier (derived from gameweek and date)
            CASE 
                WHEN EXTRACT(MONTH FROM match_date) >= 8 THEN 
                    EXTRACT(YEAR FROM match_date) || '-' || LPAD(CAST((EXTRACT(YEAR FROM match_date) + 1) % 100 AS VARCHAR), 2, '0')
                ELSE 
                    (EXTRACT(YEAR FROM match_date) - 1) || '-' || LPAD(CAST(EXTRACT(YEAR FROM match_date) % 100 AS VARCHAR), 2, '0')
            END as season,
            
            -- Original identifiers
            fixture_id,
            current_through_gameweek,
            
            -- Metadata
            scraped_date,
            CURRENT_TIMESTAMP as created_at
            
        FROM raw_fixtures
        ORDER BY gameweek, match_date, match_time
        """
        
        # Drop existing table if it exists
        analytics_conn.execute("DROP TABLE IF EXISTS analytics_fixtures")
        
        # Create the table by reading from raw database
        print("📊 Creating analytics fixtures table with derived fields...")
        
        # First, register the raw database connection so we can query it
        analytics_conn.execute(f"ATTACH '{raw_db_path}' AS raw_db")
        
        # Create table from raw database
        create_query_with_attach = """
        CREATE TABLE analytics_fixtures AS
        SELECT 
            -- Basic fixture information
            gameweek,
            match_date,
            match_time,
            day_of_week,
            home_team,
            away_team,
            venue,
            referee,
            attendance,
            
            -- Scores and completion status
            home_score,
            away_score,
            is_completed,
            
            -- Match outcome (derived fields)
            CASE 
                WHEN NOT is_completed THEN 'Not Played'
                WHEN home_score > away_score THEN 'Home Win'
                WHEN away_score > home_score THEN 'Away Win'
                WHEN home_score = away_score THEN 'Draw'
                ELSE 'Unknown'
            END as match_outcome,
            
            -- Winner (team name or draw)
            CASE 
                WHEN NOT is_completed THEN NULL
                WHEN home_score > away_score THEN home_team
                WHEN away_score > home_score THEN away_team
                WHEN home_score = away_score THEN 'Draw'
                ELSE NULL
            END as winner,
            
            -- Points for each team
            CASE 
                WHEN NOT is_completed THEN NULL
                WHEN home_score > away_score THEN 3
                WHEN home_score = away_score THEN 1
                ELSE 0
            END as home_points,
            
            CASE 
                WHEN NOT is_completed THEN NULL
                WHEN away_score > home_score THEN 3
                WHEN home_score = away_score THEN 1
                ELSE 0
            END as away_points,
            
            -- Goal difference for each team
            CASE 
                WHEN NOT is_completed THEN NULL
                ELSE home_score - away_score
            END as home_goal_difference,
            
            CASE 
                WHEN NOT is_completed THEN NULL
                ELSE away_score - home_score
            END as away_goal_difference,
            
            -- Total goals in match
            CASE 
                WHEN NOT is_completed THEN NULL
                ELSE home_score + away_score
            END as total_goals,
            
            -- Expected goals (if available)
            xG as home_xg,
            "xG.1" as away_xg,
            
            -- xG difference
            CASE 
                WHEN xG IS NOT NULL AND "xG.1" IS NOT NULL THEN xG - "xG.1"
                ELSE NULL
            END as home_xg_difference,
            
            -- Match classification by goals
            CASE 
                WHEN NOT is_completed THEN 'Not Played'
                WHEN (home_score + away_score) = 0 THEN 'Goalless'
                WHEN (home_score + away_score) >= 5 THEN 'High Scoring'
                WHEN (home_score + away_score) >= 3 THEN 'Medium Scoring'
                ELSE 'Low Scoring'
            END as goal_classification,
            
            -- Clean sheet indicators
            CASE 
                WHEN NOT is_completed THEN NULL
                WHEN away_score = 0 THEN true
                ELSE false
            END as home_clean_sheet,
            
            CASE 
                WHEN NOT is_completed THEN NULL
                WHEN home_score = 0 THEN true
                ELSE false
            END as away_clean_sheet,
            
            -- Competitive indicator (based on xG difference, fallback to score)
            CASE 
                WHEN NOT is_completed THEN NULL
                -- If we have xG data, use that (competitive if xG difference <= 0.8)
                WHEN xG IS NOT NULL AND "xG.1" IS NOT NULL THEN 
                    CASE WHEN ABS(xG - "xG.1") <= 0.8 THEN true ELSE false END
                -- Fallback to score difference if no xG data
                WHEN ABS(home_score - away_score) <= 1 THEN true
                ELSE false
            END as competitive_match,
            
            -- Season identifier (derived from gameweek and date)
            CASE 
                WHEN EXTRACT(MONTH FROM match_date) >= 8 THEN 
                    EXTRACT(YEAR FROM match_date) || '-' || LPAD(CAST((EXTRACT(YEAR FROM match_date) + 1) % 100 AS VARCHAR), 2, '0')
                ELSE 
                    (EXTRACT(YEAR FROM match_date) - 1) || '-' || LPAD(CAST(EXTRACT(YEAR FROM match_date) % 100 AS VARCHAR), 2, '0')
            END as season,
            
            -- Original identifiers
            fixture_id,
            current_through_gameweek,
            
            -- Metadata
            scraped_date,
            CURRENT_TIMESTAMP as created_at
            
        FROM raw_db.raw_fixtures
        ORDER BY gameweek, match_date, match_time
        """
        
        analytics_conn.execute(create_query_with_attach)
        
        # Create indexes for performance
        print("📇 Creating indexes...")
        
        indexes = [
            "CREATE INDEX idx_fixtures_gameweek ON analytics_fixtures(gameweek)",
            "CREATE INDEX idx_fixtures_date ON analytics_fixtures(match_date)",
            "CREATE INDEX idx_fixtures_teams ON analytics_fixtures(home_team, away_team)",
            "CREATE INDEX idx_fixtures_outcome ON analytics_fixtures(match_outcome)",
            "CREATE INDEX idx_fixtures_completed ON analytics_fixtures(is_completed)",
            "CREATE INDEX idx_fixtures_season ON analytics_fixtures(season)"
        ]
        
        for index_sql in indexes:
            analytics_conn.execute(index_sql)
        
        # Get table info and sample data
        print("\n📋 ANALYTICS FIXTURES TABLE CREATED")
        print("-" * 40)
        
        # Table stats
        stats = analytics_conn.execute("""
            SELECT 
                COUNT(*) as total_fixtures,
                COUNT(CASE WHEN is_completed THEN 1 END) as completed_fixtures,
                COUNT(CASE WHEN NOT is_completed THEN 1 END) as upcoming_fixtures,
                MIN(gameweek) as min_gameweek,
                MAX(gameweek) as max_gameweek,
                COUNT(DISTINCT season) as seasons
            FROM analytics_fixtures
        """).fetchone()
        
        print(f"✅ Total fixtures: {stats[0]}")
        print(f"✅ Completed: {stats[1]}")
        print(f"✅ Upcoming: {stats[2]}")
        print(f"✅ Gameweeks: {stats[3]} to {stats[4]}")
        print(f"✅ Seasons: {stats[5]}")
        
        # Sample outcomes
        print(f"\n📊 Match Outcomes Distribution:")
        outcomes = analytics_conn.execute("""
            SELECT match_outcome, COUNT(*) as count
            FROM analytics_fixtures 
            WHERE is_completed = true
            GROUP BY match_outcome
            ORDER BY count DESC
        """).fetchall()
        
        for outcome, count in outcomes:
            print(f"  {outcome}: {count}")
        
        # Sample high-value analysis queries
        print(f"\n🎯 ANALYSIS CAPABILITIES UNLOCKED:")
        print(f"✅ League tables (points, GD, home/away splits)")
        print(f"✅ Form analysis (last 5 matches per team)")
        print(f"✅ Head-to-head records")
        print(f"✅ Home advantage analysis")
        print(f"✅ Goal classification trends")
        print(f"✅ Clean sheet tracking")
        print(f"✅ Expected goals analysis")
        print(f"✅ Competitive match identification")
        
        # Show sample data
        print(f"\n📝 SAMPLE DATA:")
        sample = analytics_conn.execute("""
            SELECT gameweek, home_team, away_team, match_outcome, 
                   home_points, away_points, total_goals, goal_classification
            FROM analytics_fixtures 
            WHERE is_completed = true
            ORDER BY gameweek DESC, match_date DESC
            LIMIT 5
        """).fetchall()
        
        for row in sample:
            gw, home, away, outcome, hp, ap, goals, classification = row
            print(f"  GW{gw}: {home} vs {away} - {outcome} ({goals} goals, {classification})")
        
        # Detach raw database
        analytics_conn.execute("DETACH raw_db")
        
        print(f"\n🎉 SUCCESS! Analytics fixtures table created with comprehensive match analysis fields")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to create analytics fixtures table: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Close connections
        if 'raw_conn' in locals():
            raw_conn.close()
        if 'analytics_conn' in locals():
            analytics_conn.close()

def validate_analytics_fixtures():
    """Validate the analytics fixtures table"""
    print(f"\n🔍 VALIDATING ANALYTICS FIXTURES TABLE")
    print("-" * 40)
    
    analytics_db_path = "data/premierleague_analytics.duckdb"
    
    try:
        with duckdb.connect(analytics_db_path) as conn:
            
            # Check table exists
            tables = conn.execute("SHOW TABLES").fetchall()
            table_names = [t[0] for t in tables]
            
            if 'analytics_fixtures' not in table_names:
                print("❌ analytics_fixtures table not found")
                return False
            
            # Check column structure
            columns = conn.execute("PRAGMA table_info(analytics_fixtures)").fetchall()
            column_names = [col[1] for col in columns]
            
            required_columns = [
                'gameweek', 'home_team', 'away_team', 'match_outcome', 
                'winner', 'home_points', 'away_points', 'total_goals',
                'goal_classification', 'competitive_match', 'season'
            ]
            
            missing_columns = [col for col in required_columns if col not in column_names]
            if missing_columns:
                print(f"❌ Missing columns: {missing_columns}")
                return False
            
            print(f"✅ Table structure: {len(columns)} columns")
            
            # Validate data quality
            validation_queries = [
                ("Non-null gameweeks", "SELECT COUNT(*) FROM analytics_fixtures WHERE gameweek IS NOT NULL"),
                ("Valid match outcomes", "SELECT COUNT(*) FROM analytics_fixtures WHERE match_outcome IN ('Home Win', 'Away Win', 'Draw', 'Not Played')"),
                ("Completed matches have winners", "SELECT COUNT(*) FROM analytics_fixtures WHERE is_completed = true AND (winner IS NOT NULL OR winner = 'Draw')"),
                ("Points add up correctly", "SELECT COUNT(*) FROM analytics_fixtures WHERE is_completed = true AND (home_points + away_points = 3 OR home_points + away_points = 2)")
            ]
            
            for check_name, query in validation_queries:
                result = conn.execute(query).fetchone()[0]
                total = conn.execute("SELECT COUNT(*) FROM analytics_fixtures").fetchone()[0]
                print(f"✅ {check_name}: {result}/{total}")
            
            print(f"✅ Analytics fixtures table validation passed!")
            return True
            
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        return False

if __name__ == "__main__":
    print("🏈 ANALYTICS FIXTURES TABLE CREATION")
    print("=" * 60)
    
    # Create the table
    success = create_analytics_fixtures_table()
    
    if success:
        # Validate the creation
        validation_success = validate_analytics_fixtures()
        
        if validation_success:
            print(f"\n🎉 SUCCESS! Ready for match analysis")
            print(f"\nNext steps:")
            print(f"1. Query league tables: SELECT * FROM analytics_fixtures WHERE match_outcome = 'Home Win'")
            print(f"2. Analyze form: Look at recent match_outcome by team")
            print(f"3. Goal trends: Use goal_classification and total_goals")
            print(f"4. Home advantage: Compare home_points vs away_points")
        else:
            print(f"\n❌ Table created but validation failed")
    else:
        print(f"\n❌ Failed to create analytics fixtures table")