"""
Unified Column Mapping Dictionary - Built from scratch for all entity types
Maps raw FBRef columns to analytics column names for players, squads, and opponents

Designed from the ground up to be entity-aware while preserving all existing
player mapping functionality and adding squad/opponent support.
"""

# =====================================================
# PLAYER MAPPINGS (EXISTING FUNCTIONALITY PRESERVED)
# =====================================================

# Outfield Player Mappings - exactly as your original
PLAYER_OUTFIELD_MAPPINGS = {
    # ===== FROM PLAYER_STANDARD (PRIMARY SOURCE) =====
    'player_standard': {
        # Basic Info (handled separately in consolidation)
        'Player': 'player_name',
        'Nation': 'nation', 
        'Pos': 'position',
        'Squad': 'squad',
        'Age': 'age',
        'Born': 'born_year',  # Used for player_id creation
        
        # Core Playing Time
        'Playing Time MP': 'matches_played',
        'Playing Time Starts': 'starts',
        'Playing Time Min': 'minutes_played',
        'Playing Time 90s': 'minutes_90s',
        
        # Goals & Scoring (PRIMARY SOURCE - overrides duplicates)
        'Performance Gls': 'goals',
        'Performance Ast': 'assists',
        'Performance G+A': 'goals_plus_assists',
        'Performance G-PK': 'non_penalty_goals',
        'Performance PK': 'penalty_kicks_made',
        'Performance PKatt': 'penalty_kicks_attempted',
        
        # Per 90 Goals & Scoring
        'Per 90 Minutes Gls': 'goals_per_90',
        'Per 90 Minutes Ast': 'assists_per_90',
        'Per 90 Minutes G+A': 'goals_plus_assists_per_90',
        'Per 90 Minutes G-PK': 'non_penalty_goals_per_90',
        'Per 90 Minutes G+A-PK': 'goals_plus_assists_minus_pks_per_90',
        
        # Expected Goals (PRIMARY SOURCE - overrides duplicates)
        'Expected xG': 'expected_goals',
        'Expected npxG': 'non_penalty_expected_goals',
        'Expected xAG': 'expected_assisted_goals',
        'Expected npxG+xAG': 'non_penalty_xg_plus_xag',
        
        # Per 90 Expected Goals
        'Per 90 Minutes xG': 'expected_goals_per_90',
        'Per 90 Minutes xAG': 'expected_assisted_goals_per_90',
        'Per 90 Minutes xG+xAG': 'xg_plus_xag_per_90',
        'Per 90 Minutes npxG': 'non_penalty_xg_per_90',
        'Per 90 Minutes npxG+xAG': 'non_penalty_xg_plus_xag_per_90',
        
        # Progressive Actions (PRIMARY SOURCE - overrides duplicates)
        'Progression PrgC': 'progressive_carries',
        'Progression PrgP': 'progressive_passes',
        
        # Disciplinary
        'Performance CrdY': 'yellow_cards',
        'Performance CrdR': 'red_cards',
    },
    
    # ===== FROM PLAYER_SHOOTING (UNIQUE STATS ONLY) =====
    'player_shooting': {
        # SKIP: Duplicates from player_standard
        # 'Performance Gls': SKIP - use from standard
        # 'Expected xG': SKIP - use from standard
        
        # KEEP: Unique shooting stats
        'Standard Sh': 'shots',
        'Standard SoT': 'shots_on_target',
        'Standard SoT%': 'shot_accuracy',
        'Standard Sh/90': 'shots_per_90',
        'Standard SoT/90': 'shots_on_target_per_90',
        'Standard G/Sh': 'goals_per_shot',
        'Standard G/SoT': 'goals_per_shot_on_target',
        'Standard Dist': 'average_shot_distance',
        'Standard FK': 'free_kick_shots',
        'Expected G-xG': 'goals_minus_expected',
        'Expected np:G-xG': 'non_penalty_goals_minus_expected',
    },
    
    # ===== FROM PLAYER_PASSING (UNIQUE STATS ONLY) =====
    'player_passing': {
        # SKIP: Duplicates from player_standard
        # 'Expected xAG': SKIP - use from standard
        
        # KEEP: Unique passing stats
        'Total Cmp': 'passes_completed',
        'Total Att': 'passes_attempted',
        'Total Cmp%': 'pass_completion_rate',
        'Total TotDist': 'total_pass_distance',
        'Total PrgDist': 'progressive_pass_distance',
        'Short Cmp': 'short_passes_completed',
        'Short Att': 'short_passes_attempted',
        'Short Cmp%': 'short_pass_completion_rate',
        'Medium Cmp': 'medium_passes_completed',
        'Medium Att': 'medium_passes_attempted',
        'Medium Cmp%': 'medium_pass_completion_rate',
        'Long Cmp': 'long_passes_completed',
        'Long Att': 'long_passes_attempted',
        'Long Cmp%': 'long_pass_completion_rate',
        'Ast': 'assists_passing',
        'xA': 'expected_assists',
        'A-xAG': 'assists_minus_expected',
        'KP': 'key_passes',
        '1/3': 'passes_final_third',
        'PPA': 'passes_penalty_area',
        'CrsPA': 'crosses_penalty_area',
        # Skip 'PrgP': SKIP - use 'Progression PrgP' from standard
    },
    
    # ===== FROM PLAYER_PASSINGTYPES (UNIQUE STATS ONLY) =====
    'player_passingtypes': {
        # Pass Types
        'Pass Types Live': 'live_ball_passes',
        'Pass Types Dead': 'dead_ball_passes',
        'Pass Types FK': 'free_kick_passes',
        'Pass Types TB': 'through_balls',
        'Pass Types Sw': 'switches',
        'Pass Types Crs': 'crosses',
        'Pass Types TI': 'throw_ins',
        'Pass Types CK': 'corner_kicks',
        'Corner Kicks In': 'inswinging_corners',
        'Corner Kicks Out': 'outswinging_corners',
        'Corner Kicks Str': 'straight_corners',
        'Outcomes Cmp': 'completed_passes_types',
        'Outcomes Off': 'offsides_pass_types',
        'Outcomes Blocks': 'blocked_passes',
    },
    
    # ===== FROM PLAYER_GOALSHOTCREATION (UNIQUE STATS ONLY) =====
    'player_goalshotcreation': {
        'SCA SCA': 'shot_creating_actions',
        'SCA SCA90': 'shot_creating_actions_per_90',
        'SCA Types PassLive': 'sca_pass_live',
        'SCA Types PassDead': 'sca_pass_dead',
        'SCA Types TO': 'sca_take_on',
        'SCA Types Sh': 'sca_shot',
        'SCA Types Fld': 'sca_fouled',
        'SCA Types Def': 'sca_defense',
        'GCA GCA': 'goal_creating_actions',
        'GCA GCA90': 'goal_creating_actions_per_90',
        'GCA Types PassLive': 'gca_pass_live',
        'GCA Types PassDead': 'gca_pass_dead',
        'GCA Types TO': 'gca_take_on',
        'GCA Types Sh': 'gca_shot',
        'GCA Types Fld': 'gca_fouled',
        'GCA Types Def': 'gca_defense',
    },
    
    # ===== FROM PLAYER_DEFENSE (UNIQUE STATS ONLY) =====
    'player_defense': {
        'Tackles Tkl': 'tackles',
        'Tackles TklW': 'tackles_won',
        'Tackles Def 3rd': 'tackles_def_third',
        'Tackles Mid 3rd': 'tackles_mid_third',
        'Tackles Att 3rd': 'tackles_att_third',
        'Challenges Tkl': 'challenge_tackles',
        'Challenges Att': 'challenges_attempted',
        'Challenges Tkl%': 'tackle_success_rate',
        'Challenges Lost': 'challenges_lost',
        'Blocks Blocks': 'blocks',
        'Blocks Sh': 'shots_blocked',
        'Blocks Pass': 'passes_blocked',
        'Int': 'interceptions',
        'Tkl+Int': 'tackles_plus_interceptions',
        'Clr': 'clearances',
        'Err': 'errors',
    },
    
    # ===== FROM PLAYER_POSSESSION (UNIQUE STATS ONLY) =====
    'player_possession': {
        # SKIP: Duplicates from player_standard
        # 'Carries PrgC': SKIP - use 'Progression PrgC' from standard
        
        # KEEP: Unique possession stats
        'Touches Touches': 'touches',
        'Touches Def Pen': 'touches_def_penalty',
        'Touches Def 3rd': 'touches_def_third',
        'Touches Mid 3rd': 'touches_mid_third',
        'Touches Att 3rd': 'touches_att_third',
        'Touches Att Pen': 'touches_att_penalty',
        'Touches Live': 'touches_live_ball',
        'Take-Ons Att': 'take_ons_attempted',
        'Take-Ons Succ': 'take_ons_successful',
        'Take-Ons Succ%': 'take_on_success_rate',
        'Take-Ons Tkld': 'take_ons_tackled',
        'Take-Ons Tkld%': 'take_ons_tackled_rate',
        'Carries Carries': 'carries',
        'Carries TotDist': 'carry_distance',
        'Carries PrgDist': 'progressive_carry_distance',
        'Carries 1/3': 'carries_final_third',
        'Carries CPA': 'carries_penalty_area',
        'Carries Mis': 'miscontrols',
        'Carries Dis': 'dispossessed',
        'Receiving Rec': 'passes_received',
        'Receiving PrgR': 'progressive_passes_received_detail',
    },
    
    # ===== FROM PLAYER_MISC (UNIQUE STATS ONLY) =====
    'player_misc': {
        # SKIP: Duplicates from player_standard and player_defense
        # 'Performance CrdY': SKIP - use from standard
        # 'Performance CrdR': SKIP - use from standard
        # 'Performance Int': SKIP - use 'Int' from defense
        # 'Performance TklW': SKIP - use 'Tackles TklW' from defense
        
        # KEEP: Unique miscellaneous stats
        'Performance 2CrdY': 'second_yellow_cards',
        'Performance Fls': 'fouls_committed',
        'Performance Fld': 'fouls_drawn',
        'Performance Off': 'offsides',
        'Performance Crs': 'crosses_misc',
        'Performance PKwon': 'penalty_kicks_won',
        'Performance PKcon': 'penalty_kicks_conceded',
        'Performance OG': 'own_goals',
        'Performance Recov': 'ball_recoveries',
        'Aerial Duels Won': 'aerial_duels_won',
        'Aerial Duels Lost': 'aerial_duels_lost',
        'Aerial Duels Won%': 'aerial_duel_success_rate',
    },
}

# Goalkeeper Mappings - exactly as your original
PLAYER_GOALKEEPER_MAPPINGS = {
    # ===== FROM PLAYER_STANDARD (SHARED CORE STATS) =====
    'player_standard': {
        # Basic Info (handled separately)
        'Player': 'player_name',
        'Nation': 'nation',
        'Pos': 'position', 
        'Squad': 'squad',
        'Age': 'age',
        'Born': 'born_year',
        
        # Core Playing Time
        'Playing Time MP': 'matches_played',
        'Playing Time Starts': 'starts',
        'Playing Time Min': 'minutes_played',
        'Playing Time 90s': 'minutes_90s',
        
        # Core Stats (rare for keepers but for consistency)
        'Performance Gls': 'goals',
        'Performance Ast': 'assists',
        'Performance CrdY': 'yellow_cards',
        'Performance CrdR': 'red_cards',
        'Expected xG': 'expected_goals',
        'Expected npxG': 'non_penalty_expected_goals',
    },
    
    # ===== FROM PLAYER_KEEPERS =====
    'player_keepers': {
        'Performance GA': 'goals_against',
        'Performance GA90': 'goals_against_per_90',
        'Performance SoTA': 'shots_on_target_against',
        'Performance Saves': 'saves',
        'Performance Save%': 'save_percentage',
        'Performance W': 'wins',
        'Performance D': 'draws',
        'Performance L': 'losses',
        'Performance CS': 'clean_sheets',
        'Performance CS%': 'clean_sheet_percentage',
        'Penalty Kicks PKatt': 'penalty_kicks_attempted_against',
        'Penalty Kicks PKA': 'penalty_kicks_against',
        'Penalty Kicks PKsv': 'penalty_kicks_saved',
        'Penalty Kicks PKm': 'penalty_kicks_missed_by_opponent',
        'Penalty Kicks Save%': 'penalty_save_percentage',
    },
    
    # ===== FROM PLAYER_KEEPERSADV =====
    'player_keepersadv': {
        # Skip duplicate: 'Goals GA' - use from player_keepers
        'Goals PKA': 'penalty_goals_against',
        'Goals FK': 'free_kick_goals_against',
        'Goals CK': 'corner_kick_goals_against',
        'Goals OG': 'own_goals_for',
        'Expected PSxG': 'post_shot_expected_goals',
        'Expected PSxG/SoT': 'post_shot_xg_per_shot',
        'Expected PSxG+/-': 'post_shot_xg_performance',
        'Expected /90': 'post_shot_xg_performance_per_90',
        'Launched Cmp': 'goalkeeper_long_passes_completed',
        'Launched Att': 'goalkeeper_long_passes_attempted',
        'Launched Cmp%': 'goalkeeper_long_pass_accuracy',
        'Passes Att (GK)': 'goalkeeper_pass_attempts',
        'Passes Thr': 'throws',
        'Passes Launch%': 'launch_percentage',
        'Passes AvgLen': 'average_pass_length',
        'Goal Kicks Att': 'goal_kicks_attempted',
        'Goal Kicks Launch%': 'goal_kick_launch_percentage',
        'Goal Kicks AvgLen': 'goal_kick_average_length',
        'Crosses Opp': 'crosses_faced',
        'Crosses Stp': 'crosses_stopped',
        'Crosses Stp%': 'cross_stop_percentage',
        'Sweeper #OPA': 'defensive_actions_outside_penalty_area',
        'Sweeper #OPA/90': 'defensive_actions_outside_penalty_area_per_90',
        'Sweeper AvgDist': 'average_distance_defensive_actions',
    },
}

# =====================================================
# SQUAD MAPPINGS (COMBINED OUTFIELD + GOALKEEPER)
# For squads, all stats are aggregated at team level
# =====================================================

def create_squad_mappings():
    """Create squad mappings by combining outfield and goalkeeper mappings"""
    squad_mappings = {}
    
    # Convert player table names to squad table names and combine mappings
    for player_table, mappings in PLAYER_OUTFIELD_MAPPINGS.items():
        squad_table = player_table.replace('player_', 'squad_')
        squad_mappings[squad_table] = {}
        
        # Copy all mappings, but adapt for squad context
        for raw_col, analytics_col in mappings.items():
            if raw_col in ['Player', 'Nation', 'Pos', 'Born']:
                # Skip player-specific columns for squads
                continue
            elif raw_col == 'Squad':
                # Squad name mapping
                squad_mappings[squad_table]['Squad'] = 'squad_name'
            else:
                # Keep all other mappings the same
                squad_mappings[squad_table][raw_col] = analytics_col
    
    # Add goalkeeper-specific mappings to appropriate squad tables
    for player_table, mappings in PLAYER_GOALKEEPER_MAPPINGS.items():
        if player_table == 'player_standard':
            continue
        
        squad_table = player_table.replace('player_', 'squad_')
        if squad_table not in squad_mappings:
            squad_mappings[squad_table] = {}
        
        for raw_col, analytics_col in mappings.items():
            if raw_col not in ['Player', 'Nation', 'Pos', 'Squad', 'Born']:
                # SKIP if this analytics column already exists
                existing_analytics_cols = set(squad_mappings[squad_table].values())
                if analytics_col not in existing_analytics_cols:
                    squad_mappings[squad_table][raw_col] = analytics_col
    
    return squad_mappings

SQUAD_MAPPINGS = create_squad_mappings()

# =====================================================
# OPPONENT MAPPINGS (SAME AS SQUAD MAPPINGS)
# Opponent tables have identical structure to squad tables
# =====================================================

def create_opponent_mappings():
    """Create opponent mappings by adapting squad mappings"""
    opponent_mappings = {}
    
    for squad_table, mappings in SQUAD_MAPPINGS.items():
        opponent_table = squad_table.replace('squad_', 'opponent_')
        opponent_mappings[opponent_table] = {}
        
        # Copy all mappings, adapting squad_name to squad_name (opponents are still squad names)
        for raw_col, analytics_col in mappings.items():
            if raw_col == 'Squad':
                # For opponents, Squad column still maps to squad_name
                opponent_mappings[opponent_table]['Squad'] = 'squad_name'
            else:
                # Keep all other mappings the same
                opponent_mappings[opponent_table][raw_col] = analytics_col
    
    return opponent_mappings

OPPONENT_MAPPINGS = create_opponent_mappings()

# =====================================================
# UNIFIED ENTITY MAPPING SYSTEM
# =====================================================

def get_entity_mappings(entity_type: str, player_type: str = None) -> dict:
    """
    Get the appropriate mappings for an entity type
    
    Args:
        entity_type: 'player', 'squad', or 'opponent'
        player_type: For players only - 'outfield' or 'goalkeeper'
        
    Returns:
        Dictionary of table mappings for the specified entity type
    """
    if entity_type == 'player':
        if player_type == 'outfield':
            return PLAYER_OUTFIELD_MAPPINGS
        elif player_type == 'goalkeeper':
            return PLAYER_GOALKEEPER_MAPPINGS
        else:
            raise ValueError("player_type must be 'outfield' or 'goalkeeper' for entity_type='player'")
    
    elif entity_type == 'squad':
        return SQUAD_MAPPINGS
    
    elif entity_type == 'opponent':
        return OPPONENT_MAPPINGS
    
    else:
        raise ValueError("entity_type must be 'player', 'squad', or 'opponent'")

# =====================================================
# EXCLUDED COLUMNS (METADATA AND SYSTEM COLUMNS)
# =====================================================

EXCLUDED_COLUMNS = {
    # Metadata columns that shouldn't be mapped
    'Current Date', 'current_through_gameweek', 'last_updated',
    
    # Index and identifier columns
    'Rk', 'Matches',
    
    # Already handled in consolidation
    'Player', 'Squad', 'Nation', 'Pos', 'Age', 'Born',
    
    # Temporary processing columns
    'entity_key', 'player_key', 'squad_key', 'opponent_key',
}

# =====================================================
# VALIDATION FUNCTIONS
# =====================================================

def validate_all_mappings():
    """Validate mappings for all entity types"""
    
    print("🔍 VALIDATING UNIFIED ENTITY MAPPINGS")
    print("=" * 60)
    
    # Validate player mappings
    outfield_mapped = set()
    for table_mappings in PLAYER_OUTFIELD_MAPPINGS.values():
        outfield_mapped.update(table_mappings.keys())
    
    goalkeeper_mapped = set()
    for table_mappings in PLAYER_GOALKEEPER_MAPPINGS.values():
        goalkeeper_mapped.update(table_mappings.keys())
    
    # Validate squad mappings
    squad_mapped = set()
    for table_mappings in SQUAD_MAPPINGS.values():
        squad_mapped.update(table_mappings.keys())
    
    # Validate opponent mappings
    opponent_mapped = set()
    for table_mappings in OPPONENT_MAPPINGS.values():
        opponent_mapped.update(table_mappings.keys())
    
    print(f"✅ Player outfield columns mapped: {len(outfield_mapped)}")
    print(f"✅ Player goalkeeper columns mapped: {len(goalkeeper_mapped)}")
    print(f"✅ Squad columns mapped: {len(squad_mapped)}")
    print(f"✅ Opponent columns mapped: {len(opponent_mapped)}")
    print(f"✅ Total unique columns across all entities: {len(outfield_mapped | goalkeeper_mapped | squad_mapped | opponent_mapped)}")
    
    # Check for duplicate targets within each entity type
    from collections import Counter
    
    # Check player mappings
    outfield_targets = []
    for table_mappings in PLAYER_OUTFIELD_MAPPINGS.values():
        outfield_targets.extend(table_mappings.values())
    
    goalkeeper_targets = []
    for table_mappings in PLAYER_GOALKEEPER_MAPPINGS.values():
        goalkeeper_targets.extend(table_mappings.values())
    
    outfield_duplicates = [col for col, count in Counter(outfield_targets).items() if count > 1]
    goalkeeper_duplicates = [col for col, count in Counter(goalkeeper_targets).items() if count > 1]
    
    if outfield_duplicates:
        print(f"⚠️  WARNING: Duplicate outfield targets: {outfield_duplicates}")
    if goalkeeper_duplicates:
        print(f"⚠️  WARNING: Duplicate goalkeeper targets: {goalkeeper_duplicates}")
    
    if not outfield_duplicates and not goalkeeper_duplicates:
        print("✅ No conflicts in player mappings")
    
    return len(outfield_mapped), len(goalkeeper_mapped), len(squad_mapped), len(opponent_mapped)

def get_table_count_by_entity():
    """Get count of tables mapped for each entity type"""
    return {
        'player_outfield_tables': len(PLAYER_OUTFIELD_MAPPINGS),
        'player_goalkeeper_tables': len(PLAYER_GOALKEEPER_MAPPINGS),
        'squad_tables': len(SQUAD_MAPPINGS),
        'opponent_tables': len(OPPONENT_MAPPINGS)
    }

# =====================================================
# BACKWARD COMPATIBILITY
# =====================================================

# Maintain backward compatibility with existing names
OUTFIELD_PLAYER_MAPPINGS = PLAYER_OUTFIELD_MAPPINGS
GOALKEEPER_MAPPINGS = PLAYER_GOALKEEPER_MAPPINGS

# Legacy validation function
def validate_mappings():
    """Legacy validation function for backward compatibility"""
    return validate_all_mappings()

# =====================================================
# COLUMN PRIORITIES (PRESERVED FROM ORIGINAL)
# =====================================================

COLUMN_PRIORITIES = {
    # Define which table takes priority for duplicate columns
    'goals': 'player_standard',  # Not player_shooting
    'assists': 'player_standard',  # Not player_passing
    'expected_goals': 'player_standard',  # Not player_shooting
    'progressive_carries': 'player_standard',  # Not player_possession
    'progressive_passes': 'player_standard',  # Not player_passing
    'yellow_cards': 'player_standard',  # Not player_misc
    'red_cards': 'player_standard',  # Not player_misc
}

# Run validation if called directly
if __name__ == "__main__":
    validate_all_mappings()
    print(f"\n📊 Table counts: {get_table_count_by_entity()}")