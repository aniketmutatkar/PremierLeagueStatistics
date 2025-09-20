#!/usr/bin/env python3
"""
Extend Analytics Database - Add Squad and Opponent Tables
Adds new tables to existing analytics database without breaking existing functionality
"""

import duckdb
import logging
from pathlib import Path
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def extend_analytics_database():
    """Add squad and opponent tables to existing analytics database"""
    
    analytics_db_path = "data/premierleague_analytics.duckdb"
    
    print("🔧 EXTENDING ANALYTICS DATABASE")
    print("=" * 50)
    
    # Check if analytics database exists
    if not Path(analytics_db_path).exists():
        logger.error(f"❌ Analytics database not found: {analytics_db_path}")
        logger.error("❌ Please run the analytics pipeline first to create the database")
        return False
    
    try:
        with duckdb.connect(analytics_db_path) as conn:
            
            # Check existing tables
            existing_tables = conn.execute("SHOW TABLES").fetchall()
            existing_table_names = [table[0] for table in existing_tables]
            
            logger.info(f"📋 Existing tables: {existing_table_names}")
            
            # Check if new tables already exist
            new_tables = ['analytics_squads', 'analytics_opponents']
            already_exist = [table for table in new_tables if table in existing_table_names]
            
            if already_exist:
                logger.warning(f"⚠️  Tables already exist: {already_exist}")
                response = input("Do you want to drop and recreate them? (y/N): ")
                if response.lower() != 'y':
                    logger.info("❌ Aborted - no changes made")
                    return False
                
                # Drop existing tables
                for table in already_exist:
                    logger.info(f"🗑️  Dropping existing table: {table}")
                    conn.execute(f"DROP TABLE IF EXISTS {table}")
            
            # Create analytics_squads table
            logger.info("🏟️  Creating analytics_squads table...")
            conn.execute("""
                CREATE TABLE analytics_squads (
                    -- SCD TYPE 2 FRAMEWORK
                    squad_key BIGINT PRIMARY KEY,
                    squad_id VARCHAR NOT NULL,
                    squad_name VARCHAR NOT NULL,
                    season VARCHAR NOT NULL,
                    gameweek INTEGER NOT NULL,
                    valid_from DATE NOT NULL,
                    valid_to DATE,
                    is_current BOOLEAN NOT NULL,
                    
                    -- CORE PLAYING TIME & TEAM STATS
                    matches_played INTEGER,
                    starts INTEGER,
                    minutes_played INTEGER,
                    minutes_90s DECIMAL(18,3),
                    
                    -- GOALS & SCORING (team totals)
                    goals INTEGER,
                    assists INTEGER,
                    goals_plus_assists INTEGER,
                    non_penalty_goals INTEGER,
                    penalty_kicks_made INTEGER,
                    penalty_kicks_attempted INTEGER,
                    
                    -- PER 90 GOALS & SCORING
                    goals_per_90 DECIMAL(18,3),
                    assists_per_90 DECIMAL(18,3),
                    goals_plus_assists_per_90 DECIMAL(18,3),
                    non_penalty_goals_per_90 DECIMAL(18,3),
                    goals_plus_assists_minus_pks_per_90 DECIMAL(18,3),
                    
                    -- EXPECTED GOALS (team totals)
                    expected_goals DECIMAL(18,3),
                    non_penalty_expected_goals DECIMAL(18,3),
                    expected_assisted_goals DECIMAL(18,3),
                    non_penalty_xg_plus_xag DECIMAL(18,3),
                    
                    -- PER 90 EXPECTED GOALS
                    expected_goals_per_90 DECIMAL(18,3),
                    expected_assisted_goals_per_90 DECIMAL(18,3),
                    xg_plus_xag_per_90 DECIMAL(18,3),
                    non_penalty_xg_per_90 DECIMAL(18,3),
                    non_penalty_xg_plus_xag_per_90 DECIMAL(18,3),
                    
                    -- PROGRESSIVE ACTIONS
                    progressive_carries INTEGER,
                    progressive_passes INTEGER,
                    
                    -- DISCIPLINARY
                    yellow_cards INTEGER,
                    red_cards INTEGER,
                    
                    -- SHOOTING (team totals)
                    shots INTEGER,
                    shots_on_target INTEGER,
                    shot_accuracy DECIMAL(18,3),
                    shots_per_90 DECIMAL(18,3),
                    shots_on_target_per_90 DECIMAL(18,3),
                    goals_per_shot DECIMAL(18,3),
                    goals_per_shot_on_target DECIMAL(18,3),
                    average_shot_distance DECIMAL(18,3),
                    free_kick_shots INTEGER,
                    goals_minus_expected DECIMAL(18,3),
                    non_penalty_goals_minus_expected DECIMAL(18,3),
                    
                    -- PASSING (team totals)
                    passes_completed INTEGER,
                    passes_attempted INTEGER,
                    pass_completion_rate DECIMAL(18,3),
                    total_pass_distance INTEGER,
                    progressive_pass_distance INTEGER,
                    short_passes_completed INTEGER,
                    short_passes_attempted INTEGER,
                    short_pass_completion_rate DECIMAL(18,3),
                    medium_passes_completed INTEGER,
                    medium_passes_attempted INTEGER,
                    medium_pass_completion_rate DECIMAL(18,3),
                    long_passes_completed INTEGER,
                    long_passes_attempted INTEGER,
                    long_pass_completion_rate DECIMAL(18,3),
                    assists_passing INTEGER,
                    expected_assists DECIMAL(18,3),
                    assists_minus_expected DECIMAL(18,3),
                    key_passes INTEGER,
                    passes_final_third INTEGER,
                    passes_penalty_area INTEGER,
                    crosses_penalty_area INTEGER,
                    
                    -- PASS TYPES
                    live_ball_passes INTEGER,
                    dead_ball_passes INTEGER,
                    free_kick_passes INTEGER,
                    through_balls INTEGER,
                    switches INTEGER,
                    crosses INTEGER,
                    throw_ins INTEGER,
                    corner_kicks INTEGER,
                    inswinging_corners INTEGER,
                    outswinging_corners INTEGER,
                    straight_corners INTEGER,
                    completed_passes_types INTEGER,
                    offsides_pass_types INTEGER,
                    blocked_passes INTEGER,
                    
                    -- SHOT/GOAL CREATION
                    shot_creating_actions INTEGER,
                    shot_creating_actions_per_90 DECIMAL(18,3),
                    sca_pass_live INTEGER,
                    sca_pass_dead INTEGER,
                    sca_take_on INTEGER,
                    sca_shot INTEGER,
                    sca_fouled INTEGER,
                    sca_defense INTEGER,
                    goal_creating_actions INTEGER,
                    goal_creating_actions_per_90 DECIMAL(18,3),
                    gca_pass_live INTEGER,
                    gca_pass_dead INTEGER,
                    gca_take_on INTEGER,
                    gca_shot INTEGER,
                    gca_fouled INTEGER,
                    gca_defense INTEGER,
                    
                    -- DEFENSE
                    tackles INTEGER,
                    tackles_won INTEGER,
                    tackles_def_third INTEGER,
                    tackles_mid_third INTEGER,
                    tackles_att_third INTEGER,
                    challenge_tackles INTEGER,
                    challenges_attempted INTEGER,
                    tackle_success_rate DECIMAL(18,3),
                    challenges_lost INTEGER,
                    blocks INTEGER,
                    shots_blocked INTEGER,
                    passes_blocked INTEGER,
                    interceptions INTEGER,
                    tackles_plus_interceptions INTEGER,
                    clearances INTEGER,
                    errors INTEGER,
                    
                    -- POSSESSION
                    touches INTEGER,
                    touches_def_penalty INTEGER,
                    touches_def_third INTEGER,
                    touches_mid_third INTEGER,
                    touches_att_third INTEGER,
                    touches_att_penalty INTEGER,
                    touches_live_ball INTEGER,
                    take_ons_attempted INTEGER,
                    take_ons_successful INTEGER,
                    take_on_success_rate DECIMAL(18,3),
                    take_ons_tackled INTEGER,
                    take_ons_tackled_rate DECIMAL(18,3),
                    carries INTEGER,
                    carry_distance INTEGER,
                    progressive_carry_distance INTEGER,
                    carries_final_third INTEGER,
                    carries_penalty_area INTEGER,
                    miscontrols INTEGER,
                    dispossessed INTEGER,
                    passes_received INTEGER,
                    progressive_passes_received_detail INTEGER,
                    
                    -- MISCELLANEOUS
                    second_yellow_cards INTEGER,
                    fouls_committed INTEGER,
                    fouls_drawn INTEGER,
                    offsides INTEGER,
                    crosses_misc INTEGER,
                    penalty_kicks_won INTEGER,
                    penalty_kicks_conceded INTEGER,
                    own_goals INTEGER,
                    ball_recoveries INTEGER,
                    aerial_duels_won INTEGER,
                    aerial_duels_lost INTEGER,
                    aerial_duel_success_rate DECIMAL(18,3),
                    
                    -- GOALKEEPER STATS (team totals)
                    goals_against INTEGER,
                    goals_against_per_90 DECIMAL(18,3),
                    shots_on_target_against INTEGER,
                    saves INTEGER,
                    save_percentage DECIMAL(18,3),
                    wins INTEGER,
                    draws INTEGER,
                    losses INTEGER,
                    clean_sheets INTEGER,
                    clean_sheet_percentage DECIMAL(18,3),
                    penalty_kicks_attempted_against INTEGER,
                    penalty_kicks_against INTEGER,
                    penalty_kicks_saved INTEGER,
                    penalty_kicks_missed_by_opponent INTEGER,
                    penalty_save_percentage DECIMAL(18,3),
                    penalty_goals_against INTEGER,
                    free_kick_goals_against INTEGER,
                    corner_kick_goals_against INTEGER,
                    own_goals_for INTEGER,
                    post_shot_expected_goals DECIMAL(18,3),
                    post_shot_xg_per_shot DECIMAL(18,3),
                    post_shot_xg_performance DECIMAL(18,3),
                    post_shot_xg_performance_per_90 DECIMAL(18,3),
                    long_pass_accuracy DECIMAL(18,3),
                    goalkeeper_pass_attempts INTEGER,
                    throws INTEGER,
                    launch_percentage DECIMAL(18,3),
                    average_pass_length DECIMAL(18,3),
                    goal_kicks_attempted INTEGER,
                    goal_kick_launch_percentage DECIMAL(18,3),
                    goal_kick_average_length DECIMAL(18,3),
                    crosses_faced INTEGER,
                    crosses_stopped INTEGER,
                    cross_stop_percentage DECIMAL(18,3),
                    defensive_actions_outside_penalty_area INTEGER,
                    defensive_actions_outside_penalty_area_per_90 DECIMAL(18,3),
                    average_distance_defensive_actions DECIMAL(18,3),
                    
                    -- METADATA
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create analytics_opponents table (identical structure to squads)
            logger.info("🥅 Creating analytics_opponents table...")
            conn.execute("""
                CREATE TABLE analytics_opponents AS
                SELECT * FROM analytics_squads WHERE FALSE
            """)
            
            # Update the primary key name for opponents
            conn.execute("ALTER TABLE analytics_opponents RENAME COLUMN squad_key TO opponent_key")
            conn.execute("ALTER TABLE analytics_opponents RENAME COLUMN squad_id TO opponent_id")
            
            # Create indexes for performance
            logger.info("📇 Creating indexes...")
            
            # Analytics Squads Indexes
            conn.execute("CREATE INDEX idx_analytics_squads_squad_name_gameweek ON analytics_squads(squad_name, gameweek)")
            conn.execute("CREATE INDEX idx_analytics_squads_is_current ON analytics_squads(is_current)")
            conn.execute("CREATE INDEX idx_analytics_squads_season ON analytics_squads(season)")
            conn.execute("CREATE INDEX idx_analytics_squads_gameweek ON analytics_squads(gameweek)")
            
            # Analytics Opponents Indexes
            conn.execute("CREATE INDEX idx_analytics_opponents_squad_name_gameweek ON analytics_opponents(squad_name, gameweek)")
            conn.execute("CREATE INDEX idx_analytics_opponents_is_current ON analytics_opponents(is_current)")
            conn.execute("CREATE INDEX idx_analytics_opponents_season ON analytics_opponents(season)")
            conn.execute("CREATE INDEX idx_analytics_opponents_gameweek ON analytics_opponents(gameweek)")
            
            logger.info("✅ Analytics database extension completed successfully!")
            
            # Verify all tables
            all_tables = conn.execute("SHOW TABLES").fetchall()
            logger.info(f"📋 All tables now: {[table[0] for table in all_tables]}")
            
            # Show table info for new tables
            for table_name in ['analytics_squads', 'analytics_opponents']:
                count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                columns = len(conn.execute(f"PRAGMA table_info({table_name})").fetchall())
                logger.info(f"   {table_name}: {count} rows, {columns} columns")
            
        print("\n🎉 ANALYTICS DATABASE EXTENSION COMPLETE!")
        print("✅ New tables added:")
        print("   - analytics_squads: Team-level statistics with SCD Type 2")
        print("   - analytics_opponents: Opposition analysis with SCD Type 2")
        print("\n🚀 Ready to update analytics ETL pipeline!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to extend analytics database: {e}")
        import traceback
        traceback.print_exc()
        return False

def validate_extended_database():
    """Validate that the extended database is working correctly"""
    
    print("\n🔍 VALIDATING EXTENDED DATABASE")
    print("=" * 40)
    
    analytics_db_path = "data/premierleague_analytics.duckdb"
    
    try:
        with duckdb.connect(analytics_db_path) as conn:
            
            # Check all tables exist
            tables = conn.execute("SHOW TABLES").fetchall()
            table_names = [table[0] for table in tables]
            
            expected_tables = ['analytics_players', 'analytics_keepers', 'analytics_squads', 'analytics_opponents']
            missing_tables = [t for t in expected_tables if t not in table_names]
            
            if missing_tables:
                print(f"❌ Missing tables: {missing_tables}")
                return False
            
            print(f"✅ All required tables present: {expected_tables}")
            
            # Check table structures
            for table in expected_tables:
                columns = conn.execute(f"PRAGMA table_info({table})").fetchall()
                column_count = len(columns)
                print(f"   {table}: {column_count} columns")
                
                # Check for key SCD columns
                column_names = [col[1] for col in columns]
                scd_columns = ['gameweek', 'season', 'valid_from', 'valid_to', 'is_current']
                missing_scd = [col for col in scd_columns if col not in column_names]
                
                if missing_scd:
                    print(f"   ❌ Missing SCD columns in {table}: {missing_scd}")
                    return False
                else:
                    print(f"   ✅ SCD columns present in {table}")
            
            print("✅ Database extension validation successful!")
            return True
            
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        return False

if __name__ == "__main__":
    print("🚀 EXTENDING ANALYTICS DATABASE FOR UNIFIED CONSOLIDATION")
    print("=" * 70)
    
    # Extend database
    success = extend_analytics_database()
    
    if success:
        # Validate extension
        validation_success = validate_extended_database()
        
        if validation_success:
            print("\n🎉 SUCCESS! Analytics database is ready for unified consolidation")
            print("\nNext steps:")
            print("1. Update analytics ETL to use DataConsolidator")
            print("2. Add squad/opponent consolidation to ETL pipeline")
            print("3. Extend SCD processor for new entity types")
        else:
            print("\n❌ Validation failed - please check the database")
    else:
        print("\n❌ Extension failed - please check the error messages")