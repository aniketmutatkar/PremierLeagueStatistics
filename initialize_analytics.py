#!/usr/bin/env python3
"""
Analytics Database Initialization Script
Run this to create the analytics schema in your new analytics database
"""

import duckdb
import yaml
from pathlib import Path

def load_config():
    """Load database configuration"""
    config_path = Path("config/database.yaml")
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def get_analytics_schema_sql():
    """Return the complete analytics schema SQL"""
    return """
-- ================================================================================
-- ANALYTICS LAYER SCHEMA - Premier League Statistics
-- ================================================================================

CREATE TABLE IF NOT EXISTS analytics_players (
    -- SCD Type 2 Keys
    player_key BIGINT PRIMARY KEY,
    player_id VARCHAR NOT NULL,
    
    -- Dimensions
    player_name VARCHAR NOT NULL,
    squad VARCHAR NOT NULL,
    position VARCHAR,
    nation VARCHAR,
    age DECIMAL,
    
    -- Time Dimensions (SCD Type 2)
    season VARCHAR NOT NULL,
    gameweek INTEGER NOT NULL,
    valid_from DATE NOT NULL,
    valid_to DATE,
    is_current BOOLEAN NOT NULL DEFAULT FALSE,
    
    -- Playing Time & Basic Info
    matches_played INTEGER DEFAULT 0,
    starts INTEGER DEFAULT 0,
    minutes_played INTEGER DEFAULT 0,
    minutes_90s DECIMAL DEFAULT 0,
    
    -- Performance Stats
    goals INTEGER DEFAULT 0,
    assists INTEGER DEFAULT 0,
    goals_plus_assists INTEGER DEFAULT 0,
    non_penalty_goals INTEGER DEFAULT 0,
    penalty_kicks_made INTEGER DEFAULT 0,
    penalty_kicks_attempted INTEGER DEFAULT 0,
    yellow_cards INTEGER DEFAULT 0,
    red_cards INTEGER DEFAULT 0,
    
    -- Expected Stats
    expected_goals DECIMAL DEFAULT 0,
    non_penalty_expected_goals DECIMAL DEFAULT 0,
    expected_assisted_goals DECIMAL DEFAULT 0,
    expected_goals_plus_assists DECIMAL DEFAULT 0,
    
    -- Progression
    progressive_carries INTEGER DEFAULT 0,
    progressive_passes INTEGER DEFAULT 0,
    progressive_passes_received INTEGER DEFAULT 0,
    
    -- Shooting Stats
    shots INTEGER DEFAULT 0,
    shots_on_target INTEGER DEFAULT 0,
    shot_accuracy DECIMAL DEFAULT 0,
    shots_per_90 DECIMAL DEFAULT 0,
    shots_on_target_per_90 DECIMAL DEFAULT 0,
    goals_per_shot DECIMAL DEFAULT 0,
    goals_per_shot_on_target DECIMAL DEFAULT 0,
    average_shot_distance DECIMAL DEFAULT 0,
    free_kick_shots INTEGER DEFAULT 0,
    
    -- Passing Stats
    passes_completed INTEGER DEFAULT 0,
    passes_attempted INTEGER DEFAULT 0,
    pass_accuracy DECIMAL DEFAULT 0,
    total_pass_distance DECIMAL DEFAULT 0,
    progressive_pass_distance DECIMAL DEFAULT 0,
    short_passes_completed INTEGER DEFAULT 0,
    short_passes_attempted INTEGER DEFAULT 0,
    short_pass_accuracy DECIMAL DEFAULT 0,
    medium_passes_completed INTEGER DEFAULT 0,
    medium_passes_attempted INTEGER DEFAULT 0,
    medium_pass_accuracy DECIMAL DEFAULT 0,
    long_passes_completed INTEGER DEFAULT 0,
    long_passes_attempted INTEGER DEFAULT 0,
    long_pass_accuracy DECIMAL DEFAULT 0,
    key_passes INTEGER DEFAULT 0,
    final_third_passes INTEGER DEFAULT 0,
    penalty_area_passes INTEGER DEFAULT 0,
    crosses INTEGER DEFAULT 0,
    
    -- Pass Types
    live_ball_passes INTEGER DEFAULT 0,
    dead_ball_passes INTEGER DEFAULT 0,
    through_balls INTEGER DEFAULT 0,
    switches INTEGER DEFAULT 0,
    throw_ins INTEGER DEFAULT 0,
    corner_kicks INTEGER DEFAULT 0,
    
    -- Goal and Shot Creation
    shot_creating_actions INTEGER DEFAULT 0,
    shot_creating_actions_per_90 DECIMAL DEFAULT 0,
    goal_creating_actions INTEGER DEFAULT 0,
    goal_creating_actions_per_90 DECIMAL DEFAULT 0,
    
    -- Defensive Stats
    tackles INTEGER DEFAULT 0,
    tackles_won INTEGER DEFAULT 0,
    tackles_def_third INTEGER DEFAULT 0,
    tackles_mid_third INTEGER DEFAULT 0,
    tackles_att_third INTEGER DEFAULT 0,
    tackle_success_rate DECIMAL DEFAULT 0,
    blocks INTEGER DEFAULT 0,
    shots_blocked INTEGER DEFAULT 0,
    passes_blocked INTEGER DEFAULT 0,
    interceptions INTEGER DEFAULT 0,
    tackles_plus_interceptions INTEGER DEFAULT 0,
    clearances INTEGER DEFAULT 0,
    errors INTEGER DEFAULT 0,
    
    -- Possession Stats
    possession_percentage DECIMAL DEFAULT 0,
    touches INTEGER DEFAULT 0,
    touches_def_penalty INTEGER DEFAULT 0,
    touches_def_third INTEGER DEFAULT 0,
    touches_mid_third INTEGER DEFAULT 0,
    touches_att_third INTEGER DEFAULT 0,
    touches_att_penalty INTEGER DEFAULT 0,
    take_ons_attempted INTEGER DEFAULT 0,
    take_ons_successful INTEGER DEFAULT 0,
    take_on_success_rate DECIMAL DEFAULT 0,
    carries INTEGER DEFAULT 0,
    carry_distance DECIMAL DEFAULT 0,
    progressive_carry_distance DECIMAL DEFAULT 0,
    carries_final_third INTEGER DEFAULT 0,
    carries_penalty_area INTEGER DEFAULT 0,
    miscontrols INTEGER DEFAULT 0,
    dispossessed INTEGER DEFAULT 0,
    
    -- Miscellaneous Stats
    fouls_committed INTEGER DEFAULT 0,
    fouls_drawn INTEGER DEFAULT 0,
    offsides INTEGER DEFAULT 0,
    second_yellows INTEGER DEFAULT 0,
    penalties_won INTEGER DEFAULT 0,
    penalties_conceded INTEGER DEFAULT 0,
    own_goals INTEGER DEFAULT 0,
    ball_recoveries INTEGER DEFAULT 0,
    aerial_duels_won INTEGER DEFAULT 0,
    aerial_duels_lost INTEGER DEFAULT 0,
    
    -- Goalkeeper Stats
    goals_against INTEGER DEFAULT 0,
    goals_against_per_90 DECIMAL DEFAULT 0,
    shots_on_target_against INTEGER DEFAULT 0,
    saves INTEGER DEFAULT 0,
    save_percentage DECIMAL DEFAULT 0,
    wins INTEGER DEFAULT 0,
    draws INTEGER DEFAULT 0,
    losses INTEGER DEFAULT 0,
    clean_sheets INTEGER DEFAULT 0,
    clean_sheet_percentage DECIMAL DEFAULT 0,
    penalties_faced INTEGER DEFAULT 0,
    penalties_saved INTEGER DEFAULT 0,
    post_shot_expected_goals DECIMAL DEFAULT 0,
    post_shot_expected_goals_per_shot DECIMAL DEFAULT 0,
    psxg_performance DECIMAL DEFAULT 0,
    cross_stopping_percentage DECIMAL DEFAULT 0,
    sweeper_actions INTEGER DEFAULT 0,
    sweeper_actions_per_90 DECIMAL DEFAULT 0,
    keeper_pass_accuracy DECIMAL DEFAULT 0,
    
    -- DERIVED METRICS
    goals_vs_expected DECIMAL DEFAULT 0,
    npgoals_vs_expected DECIMAL DEFAULT 0,
    key_pass_conversion DECIMAL DEFAULT 0,
    expected_goal_involvement_per_90 DECIMAL DEFAULT 0,
    progressive_actions_per_90 DECIMAL DEFAULT 0,
    possession_efficiency DECIMAL DEFAULT 0,
    final_third_involvement DECIMAL DEFAULT 0,
    defensive_actions_per_90 DECIMAL DEFAULT 0,
    aerial_duel_success_rate DECIMAL DEFAULT 0,
    goals_last_5gw INTEGER DEFAULT 0,
    assists_last_5gw INTEGER DEFAULT 0,
    form_score DECIMAL DEFAULT 0,
    goal_share_of_team DECIMAL DEFAULT 0,
    assist_share_of_team DECIMAL DEFAULT 0,
    minutes_percentage_of_team DECIMAL DEFAULT 0,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics_teams (
    team_key BIGINT PRIMARY KEY,
    team_id VARCHAR NOT NULL,
    team_name VARCHAR NOT NULL,
    season VARCHAR NOT NULL,
    gameweek INTEGER NOT NULL,
    valid_from DATE NOT NULL,
    valid_to DATE,
    is_current BOOLEAN NOT NULL DEFAULT FALSE,
    
    -- Team Performance Stats
    matches_played INTEGER DEFAULT 0,
    wins INTEGER DEFAULT 0,
    draws INTEGER DEFAULT 0,
    losses INTEGER DEFAULT 0,
    points INTEGER DEFAULT 0,
    goals_for INTEGER DEFAULT 0,
    goals_against INTEGER DEFAULT 0,
    goal_difference INTEGER DEFAULT 0,
    expected_goals_for DECIMAL DEFAULT 0,
    expected_goals_against DECIMAL DEFAULT 0,
    goals_vs_expected_for DECIMAL DEFAULT 0,
    goals_vs_expected_against DECIMAL DEFAULT 0,
    shots INTEGER DEFAULT 0,
    shots_on_target INTEGER DEFAULT 0,
    shot_accuracy DECIMAL DEFAULT 0,
    shots_per_game DECIMAL DEFAULT 0,
    pass_accuracy DECIMAL DEFAULT 0,
    progressive_passes INTEGER DEFAULT 0,
    key_passes INTEGER DEFAULT 0,
    crosses INTEGER DEFAULT 0,
    tackles INTEGER DEFAULT 0,
    interceptions INTEGER DEFAULT 0,
    blocks INTEGER DEFAULT 0,
    clearances INTEGER DEFAULT 0,
    clean_sheets INTEGER DEFAULT 0,
    clean_sheet_percentage DECIMAL DEFAULT 0,
    possession_percentage DECIMAL DEFAULT 0,
    touches INTEGER DEFAULT 0,
    yellow_cards INTEGER DEFAULT 0,
    red_cards INTEGER DEFAULT 0,
    fouls_committed INTEGER DEFAULT 0,
    fouls_drawn INTEGER DEFAULT 0,
    points_last_5_games INTEGER DEFAULT 0,
    goals_for_last_5_games INTEGER DEFAULT 0,
    goals_against_last_5_games INTEGER DEFAULT 0,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics_teams_defense (
    team_defense_key BIGINT PRIMARY KEY,
    team_id VARCHAR NOT NULL,
    team_name VARCHAR NOT NULL,
    season VARCHAR NOT NULL,
    gameweek INTEGER NOT NULL,
    valid_from DATE NOT NULL,
    valid_to DATE,
    is_current BOOLEAN NOT NULL DEFAULT FALSE,
    
    opponent_goals INTEGER DEFAULT 0,
    opponent_shots INTEGER DEFAULT 0,
    opponent_shots_on_target INTEGER DEFAULT 0,
    opponent_shot_accuracy DECIMAL DEFAULT 0,
    opponent_expected_goals DECIMAL DEFAULT 0,
    opponent_pass_accuracy DECIMAL DEFAULT 0,
    opponent_progressive_passes INTEGER DEFAULT 0,
    opponent_key_passes INTEGER DEFAULT 0,
    opponent_final_third_passes INTEGER DEFAULT 0,
    opponent_penalty_area_passes INTEGER DEFAULT 0,
    defensive_actions_allowed INTEGER DEFAULT 0,
    possession_allowed DECIMAL DEFAULT 0,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes (without WHERE clauses - DuckDB doesn't support partial indexes yet)
CREATE INDEX IF NOT EXISTS idx_analytics_players_current 
ON analytics_players(is_current, gameweek);

CREATE INDEX IF NOT EXISTS idx_analytics_players_player_gameweek 
ON analytics_players(player_id, gameweek);

CREATE INDEX IF NOT EXISTS idx_analytics_players_squad_position 
ON analytics_players(squad, position, gameweek);

CREATE INDEX IF NOT EXISTS idx_analytics_teams_current 
ON analytics_teams(is_current, gameweek);

CREATE INDEX IF NOT EXISTS idx_analytics_teams_team_gameweek 
ON analytics_teams(team_id, gameweek);

CREATE INDEX IF NOT EXISTS idx_analytics_teams_defense_current 
ON analytics_teams_defense(is_current, gameweek);

CREATE INDEX IF NOT EXISTS idx_analytics_teams_defense_team_gameweek 
ON analytics_teams_defense(team_id, gameweek);
"""

def initialize_analytics_database():
    """Create analytics schema in the analytics database"""
    print("🚀 Initializing Analytics Database...")
    
    # Load config
    config = load_config()
    analytics_db_path = config['database']['paths']['analytics']
    
    print(f"📂 Connecting to: {analytics_db_path}")
    
    # Connect to analytics database
    conn = duckdb.connect(analytics_db_path)
    
    try:
        # Execute schema creation
        schema_sql = get_analytics_schema_sql()
        conn.execute(schema_sql)
        
        print("✅ Analytics schema created successfully!")
        
        # Verify tables were created
        tables = conn.execute("SHOW TABLES").fetchall()
        analytics_tables = [table[0] for table in tables if table[0].startswith('analytics_')]
        
        print(f"📊 Created {len(analytics_tables)} analytics tables:")
        for table in analytics_tables:
            row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"   - {table}: {row_count} rows")
        
        return True
        
    except Exception as e:
        print(f"❌ Error creating analytics schema: {e}")
        return False
        
    finally:
        conn.close()

if __name__ == "__main__":
    success = initialize_analytics_database()
    if success:
        print("\n🎉 Analytics database is ready!")
        print("Next step: Build the ETL pipeline to populate with data")
    else:
        print("\n💥 Failed to initialize analytics database")