#!/usr/bin/env python3
"""
Create Fresh Analytics Database
Drops the old analytics database and creates a clean one with new schemas
"""

import duckdb
import logging
from pathlib import Path
import shutil
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_fresh_analytics_database():
    """Create a fresh analytics database with new schemas"""
    
    analytics_db_path = "data/premierleague_analytics.duckdb"
    backup_path = f"data/premierleague_analytics_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.duckdb"
    
    print("🗃️  CREATING FRESH ANALYTICS DATABASE")
    print("=" * 50)
    
    # Step 1: Backup existing database if it exists
    if Path(analytics_db_path).exists():
        logger.info(f"📦 Backing up existing database to {backup_path}")
        shutil.copy2(analytics_db_path, backup_path)
        
        # Remove old database
        Path(analytics_db_path).unlink()
        logger.info(f"🗑️  Removed old analytics database")
    else:
        logger.info("📄 No existing analytics database found")
    
    # Step 2: Create fresh database
    logger.info(f"🆕 Creating fresh analytics database: {analytics_db_path}")
    
    try:
        with duckdb.connect(analytics_db_path) as conn:
            
            # Create analytics_players table (outfield players)
            logger.info("📊 Creating analytics_players table...")
            conn.execute("""
                CREATE TABLE analytics_players (
                    -- SCD TYPE 2 FRAMEWORK
                    player_key BIGINT PRIMARY KEY,
                    player_id VARCHAR NOT NULL,
                    player_name VARCHAR NOT NULL,
                    squad VARCHAR NOT NULL,
                    born_year VARCHAR NOT NULL,
                    position VARCHAR, -- DF, MF, FW (excluding GK)
                    nation VARCHAR,
                    age DECIMAL(18,3),
                    season VARCHAR NOT NULL,
                    gameweek INTEGER NOT NULL,
                    valid_from DATE NOT NULL,
                    valid_to DATE,
                    is_current BOOLEAN NOT NULL,
                    
                    -- CORE PLAYING TIME
                    matches_played INTEGER,
                    starts INTEGER,
                    minutes_played INTEGER,
                    minutes_90s DECIMAL(18,3),
                    
                    -- GOALS & SCORING
                    goals INTEGER,
                    assists INTEGER,
                    goals_plus_assists INTEGER,
                    non_penalty_goals INTEGER,
                    penalty_kicks_made INTEGER,
                    penalty_kicks_attempted INTEGER,
                    goals_per_90 DECIMAL(18,3),
                    assists_per_90 DECIMAL(18,3),
                    goals_plus_assists_per_90 DECIMAL(18,3),
                    non_penalty_goals_per_90 DECIMAL(18,3),
                    goals_plus_assists_minus_pks_per_90 DECIMAL(18,3),
                    
                    -- EXPECTED GOALS
                    expected_goals DECIMAL(18,3),
                    non_penalty_expected_goals DECIMAL(18,3),
                    expected_assisted_goals DECIMAL(18,3),
                    non_penalty_xg_plus_xag DECIMAL(18,3),
                    expected_goals_per_90 DECIMAL(18,3),
                    expected_assisted_goals_per_90 DECIMAL(18,3),
                    xg_plus_xag_per_90 DECIMAL(18,3),
                    non_penalty_xg_per_90 DECIMAL(18,3),
                    non_penalty_xg_plus_xag_per_90 DECIMAL(18,3),
                    
                    -- PROGRESSIVE ACTIONS
                    progressive_carries INTEGER,
                    progressive_passes INTEGER,
                    progressive_passes_received INTEGER,
                    
                    -- DISCIPLINE
                    yellow_cards INTEGER,
                    red_cards INTEGER,
                    
                    -- SHOOTING
                    shots INTEGER,
                    shots_on_target INTEGER,
                    shot_accuracy DECIMAL(18,3),
                    shots_per_90 DECIMAL(18,3),
                    shots_on_target_per_90 DECIMAL(18,3),
                    goals_per_shot DECIMAL(18,3),
                    goals_per_shot_on_target DECIMAL(18,3),
                    average_shot_distance DECIMAL(18,3),
                    free_kick_shots INTEGER,
                    non_penalty_xg_per_shot DECIMAL(18,3),
                    goals_minus_expected_goals DECIMAL(18,3),
                    non_penalty_goals_minus_expected DECIMAL(18,3),
                    
                    -- PASSING
                    passes_completed INTEGER,
                    passes_attempted INTEGER,
                    pass_accuracy DECIMAL(18,3),
                    total_pass_distance INTEGER,
                    progressive_pass_distance INTEGER,
                    short_passes_completed INTEGER,
                    short_passes_attempted INTEGER,
                    short_pass_accuracy DECIMAL(18,3),
                    medium_passes_completed INTEGER,
                    medium_passes_attempted INTEGER,
                    medium_pass_accuracy DECIMAL(18,3),
                    long_passes_completed INTEGER,
                    long_passes_attempted INTEGER,
                    long_pass_accuracy DECIMAL(18,3),
                    expected_assisted_goals_passing DECIMAL(18,3),
                    expected_assists DECIMAL(18,3),
                    assists_minus_expected DECIMAL(18,3),
                    key_passes INTEGER,
                    final_third_passes INTEGER,
                    penalty_area_passes INTEGER,
                    crosses_into_penalty_area INTEGER,
                    
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
                    passes_offside INTEGER,
                    passes_blocked INTEGER,
                    
                    -- SHOT/GOAL CREATION
                    shot_creating_actions INTEGER,
                    shot_creating_actions_per_90 DECIMAL(18,3),
                    sca_pass_live INTEGER,
                    sca_pass_dead INTEGER,
                    sca_take_ons INTEGER,
                    sca_shots INTEGER,
                    sca_fouls_drawn INTEGER,
                    sca_defensive_actions INTEGER,
                    goal_creating_actions INTEGER,
                    goal_creating_actions_per_90 DECIMAL(18,3),
                    gca_pass_live INTEGER,
                    gca_pass_dead INTEGER,
                    gca_take_ons INTEGER,
                    gca_shots INTEGER,
                    gca_fouls_drawn INTEGER,
                    gca_defensive_actions INTEGER,
                    
                    -- DEFENSE
                    tackles INTEGER,
                    tackles_won INTEGER,
                    tackles_def_third INTEGER,
                    tackles_mid_third INTEGER,
                    tackles_att_third INTEGER,
                    challenges_attempted INTEGER,
                    challenges_total INTEGER,
                    tackle_success_rate DECIMAL(18,3),
                    challenges_lost INTEGER,
                    blocks INTEGER,
                    shots_blocked INTEGER,
                    passes_blocked_defense INTEGER,
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
                    
                    -- METADATA
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create analytics_keepers table (goalkeepers)
            logger.info("🥅 Creating analytics_keepers table...")
            conn.execute("""
                CREATE TABLE analytics_keepers (
                    -- SCD TYPE 2 FRAMEWORK
                    player_key BIGINT PRIMARY KEY,
                    player_id VARCHAR NOT NULL,
                    player_name VARCHAR NOT NULL,
                    squad VARCHAR NOT NULL,
                    born_year VARCHAR NOT NULL,
                    position VARCHAR DEFAULT 'GK',
                    nation VARCHAR,
                    age DECIMAL(18,3),
                    season VARCHAR NOT NULL,
                    gameweek INTEGER NOT NULL,
                    valid_from DATE NOT NULL,
                    valid_to DATE,
                    is_current BOOLEAN NOT NULL,
                    
                    -- CORE PLAYING TIME
                    matches_played INTEGER,
                    starts INTEGER,
                    minutes_played INTEGER,
                    minutes_90s DECIMAL(18,3),
                    
                    -- CORE STATS (for consistency)
                    goals INTEGER,
                    assists INTEGER,
                    yellow_cards INTEGER,
                    red_cards INTEGER,
                    expected_goals DECIMAL(18,3),
                    non_penalty_expected_goals DECIMAL(18,3),
                    
                    -- BASIC GOALKEEPER STATS
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
                    
                    -- ADVANCED GOALKEEPER STATS
                    penalty_goals_against INTEGER,
                    free_kick_goals_against INTEGER,
                    corner_kick_goals_against INTEGER,
                    own_goals_for INTEGER,
                    post_shot_expected_goals DECIMAL(18,3),
                    post_shot_xg_per_shot DECIMAL(18,3),
                    post_shot_xg_performance DECIMAL(18,3),
                    post_shot_xg_performance_per_90 DECIMAL(18,3),
                    long_passes_completed INTEGER,
                    long_passes_attempted INTEGER,
                    long_pass_accuracy DECIMAL(18,3),
                    goalkeeper_pass_attempts INTEGER,
                    throws INTEGER,
                    launch_percentage DECIMAL(18,3),
                    average_pass_length DECIMAL(18,3),
                    goal_kick_attempts INTEGER,
                    goal_kick_launch_percentage DECIMAL(18,3),
                    goal_kick_average_length DECIMAL(18,3),
                    crosses_faced INTEGER,
                    crosses_stopped INTEGER,
                    cross_stopping_percentage DECIMAL(18,3),
                    sweeper_actions INTEGER,
                    sweeper_actions_per_90 DECIMAL(18,3),
                    sweeper_average_distance DECIMAL(18,3),
                    
                    -- METADATA
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes for performance
            logger.info("📇 Creating indexes...")
            
            # Analytics Players Indexes
            conn.execute("CREATE INDEX idx_analytics_players_squad_gameweek ON analytics_players(squad, gameweek)")
            conn.execute("CREATE INDEX idx_analytics_players_position_gameweek ON analytics_players(position, gameweek)")
            conn.execute("CREATE INDEX idx_analytics_players_is_current ON analytics_players(is_current)")
            conn.execute("CREATE INDEX idx_analytics_players_season ON analytics_players(season)")
            conn.execute("CREATE INDEX idx_analytics_players_player_name ON analytics_players(player_name)")
            
            # Analytics Keepers Indexes
            conn.execute("CREATE INDEX idx_analytics_keepers_squad_gameweek ON analytics_keepers(squad, gameweek)")
            conn.execute("CREATE INDEX idx_analytics_keepers_is_current ON analytics_keepers(is_current)")
            conn.execute("CREATE INDEX idx_analytics_keepers_season ON analytics_keepers(season)")
            conn.execute("CREATE INDEX idx_analytics_keepers_player_name ON analytics_keepers(player_name)")
            
            # Skip constraints - DuckDB doesn't support ALTER TABLE ADD CONSTRAINT yet
            logger.info("ℹ️  Skipping constraints (not supported in DuckDB yet)")
            
            logger.info("✅ Fresh analytics database created successfully!")
            
            # Verify tables
            tables = conn.execute("SHOW TABLES").fetchall()
            logger.info(f"📋 Created tables: {[table[0] for table in tables]}")
            
            # Show table info
            for table_name in ['analytics_players', 'analytics_keepers']:
                count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                columns = len(conn.execute(f"PRAGMA table_info({table_name})").fetchall())
                logger.info(f"   {table_name}: {count} rows, {columns} columns")
        
        print("\n🎉 FRESH ANALYTICS DATABASE CREATION COMPLETE!")
        print("=" * 50)
        print(f"✅ New database: {analytics_db_path}")
        if Path(backup_path).exists():
            print(f"📦 Backup saved: {backup_path}")
        print("🚀 Ready for analytics pipeline!")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error creating fresh database: {e}")
        return False

if __name__ == "__main__":
    success = create_fresh_analytics_database()
    exit(0 if success else 1)