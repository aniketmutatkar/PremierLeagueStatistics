#!/usr/bin/env python3
"""
Create Complete Analytics Database - All Entity Types
Creates fresh analytics database with all tables: players, keepers, squads, opponents
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

def create_complete_analytics_database():
    """Create a complete analytics database with all entity types"""
    
    analytics_db_path = "data/premierleague_analytics.duckdb"
    backup_path = f"data/premierleague_analytics_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.duckdb"
    
    print("🗃️  CREATING COMPLETE ANALYTICS DATABASE")
    print("=" * 60)
    
    # Step 1: Backup existing database if it exists
    if Path(analytics_db_path).exists():
        logger.info(f"📦 Backing up existing database to {backup_path}")
        shutil.copy2(analytics_db_path, backup_path)
        
        # Remove old database
        Path(analytics_db_path).unlink()
        logger.info(f"🗑️  Removed old analytics database")
    else:
        logger.info("📄 No existing analytics database found")
    
    # Step 2: Create fresh database with all tables
    logger.info(f"🆕 Creating complete analytics database: {analytics_db_path}")
    
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
                    
                    -- PER 90 GOALS & SCORING
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
                    goals_minus_expected DECIMAL(18,3),
                    non_penalty_goals_minus_expected DECIMAL(18,3),
                    
                    -- PASSING
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
                    
                    -- BASIC STATS (rare for keepers but included for consistency)
                    goals INTEGER,
                    assists INTEGER,
                    yellow_cards INTEGER,
                    red_cards INTEGER,
                    expected_goals DECIMAL(18,3),
                    non_penalty_expected_goals DECIMAL(18,3),
                    
                    -- GOALKEEPING STATS
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
                    
                    -- ADVANCED GOALKEEPING
                    penalty_goals_against INTEGER,
                    free_kick_goals_against INTEGER,
                    corner_kick_goals_against INTEGER,
                    own_goals_for INTEGER,
                    post_shot_expected_goals DECIMAL(18,3),
                    post_shot_xg_per_shot DECIMAL(18,3),
                    post_shot_xg_performance DECIMAL(18,3),
                    post_shot_xg_performance_per_90 DECIMAL(18,3),
                    
                    -- GOALKEEPER DISTRIBUTION
                    long_passes_completed INTEGER,
                    long_passes_attempted INTEGER,
                    long_pass_accuracy DECIMAL(18,3),
                    goalkeeper_pass_attempts INTEGER,
                    throws INTEGER,
                    launch_percentage DECIMAL(18,3),
                    average_pass_length DECIMAL(18,3),
                    goal_kicks_attempted INTEGER,
                    goal_kick_launch_percentage DECIMAL(18,3),
                    goal_kick_average_length DECIMAL(18,3),
                    
                    -- GOALKEEPER ACTIONS
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
            
            # Create analytics_squads table (team-level stats)
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
                CREATE TABLE analytics_opponents (
                    -- SCD TYPE 2 FRAMEWORK
                    opponent_key BIGINT PRIMARY KEY,
                    opponent_id VARCHAR NOT NULL,
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
            
            # Create indexes for performance
            logger.info("📇 Creating indexes for all tables...")
            
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
            
            # Skip constraints - DuckDB doesn't support ALTER TABLE ADD CONSTRAINT yet
            logger.info("ℹ️  Skipping constraints (not supported in DuckDB yet)")
            
            logger.info("✅ Complete analytics database created successfully!")
            
            # Verify all tables
            tables = conn.execute("SHOW TABLES").fetchall()
            logger.info(f"📋 Created tables: {[table[0] for table in tables]}")
            
            # Show table info for all tables
            expected_tables = ['analytics_players', 'analytics_keepers', 'analytics_squads', 'analytics_opponents']
            for table_name in expected_tables:
                count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                columns = len(conn.execute(f"PRAGMA table_info({table_name})").fetchall())
                logger.info(f"   {table_name}: {count} rows, {columns} columns")
        
        print("\n🎉 COMPLETE ANALYTICS DATABASE CREATION FINISHED!")
        print("=" * 60)
        print("✅ All tables created:")
        print("   - analytics_players: Outfield players with comprehensive statistics")
        print("   - analytics_keepers: Goalkeepers with specialized metrics")
        print("   - analytics_squads: Team-level statistics with SCD Type 2")
        print("   - analytics_opponents: Opposition analysis with SCD Type 2")
        print("\n🚀 Database is ready for unified consolidation pipeline!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to create complete analytics database: {e}")
        import traceback
        traceback.print_exc()
        return False

def validate_complete_database():
    """Validate that the complete database is working correctly"""
    
    print("\n🔍 VALIDATING COMPLETE DATABASE")
    print("=" * 50)
    
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
            
            # Check table structures and SCD columns
            for table in expected_tables:
                columns = conn.execute(f"PRAGMA table_info({table})").fetchall()
                column_count = len(columns)
                column_names = [col[1] for col in columns]
                
                print(f"   {table}: {column_count} columns")
                
                # Check for key SCD columns
                scd_columns = ['gameweek', 'season', 'valid_from', 'valid_to', 'is_current']
                missing_scd = [col for col in scd_columns if col not in column_names]
                
                if missing_scd:
                    print(f"   ❌ Missing SCD columns in {table}: {missing_scd}")
                    return False
                else:
                    print(f"   ✅ SCD columns present in {table}")
                
                # Check for entity-specific key columns
                if 'player' in table and 'player_key' not in column_names:
                    print(f"   ❌ Missing player_key in {table}")
                    return False
                elif 'squad' in table and 'squad_key' not in column_names:
                    print(f"   ❌ Missing squad_key in {table}")
                    return False
                elif 'opponent' in table and 'opponent_key' not in column_names:
                    print(f"   ❌ Missing opponent_key in {table}")
                    return False
                else:
                    print(f"   ✅ Primary key column present in {table}")
            
            # Check indexes exist
            print(f"\n📇 Checking indexes...")
            index_count = 0
            for table in expected_tables:
                try:
                    indexes = conn.execute(f"PRAGMA index_list({table})").fetchall()
                    table_index_count = len(indexes)
                    index_count += table_index_count
                    print(f"   {table}: {table_index_count} indexes")
                except:
                    print(f"   {table}: No indexes found")
            
            print(f"✅ Total indexes created: {index_count}")
            
            print("✅ Complete database validation successful!")
            return True
            
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        return False

if __name__ == "__main__":
    print("🚀 CREATING COMPLETE ANALYTICS DATABASE FOR UNIFIED SYSTEM")
    print("=" * 80)
    print("This will create all analytics tables: players, keepers, squads, opponents")
    print("=" * 80)
    
    # Confirm before proceeding
    response = input("\nThis will backup and replace your existing analytics database. Continue? (y/N): ")
    if response.lower() != 'y':
        print("❌ Aborted - no changes made")
        exit(1)
    
    # Create complete database
    success = create_complete_analytics_database()
    
    if success:
        # Validate creation
        validation_success = validate_complete_database()
        
        if validation_success:
            print("\n🎉 SUCCESS! Complete analytics database is ready")
            print("\nNext steps:")
            print("1. Test the unified analytics ETL pipeline")
            print("2. Verify data consolidation for all entity types")
            print("3. Run system validation")
            print("\nYou can now run: python pipelines/analytics_pipeline.py --force")
        else:
            print("\n❌ Validation failed - please check the database")
    else:
        print("\n❌ Creation failed - please check the error messages")