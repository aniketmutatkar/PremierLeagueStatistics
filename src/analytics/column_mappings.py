"""
Complete Column Mapping Dictionary
Maps every raw FBRef column to its corresponding analytics column name

This dictionary resolves all duplicates using our priority system:
- player_standard = PRIMARY source for core stats
- Specialist tables = SECONDARY for unique stats only
"""

# =====================================================
# OUTFIELD PLAYER MAPPINGS
# Maps raw columns -> analytics_players table columns
# =====================================================

OUTFIELD_PLAYER_MAPPINGS = {
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
        'Progression PrgR': 'progressive_passes_received',
        
        # Discipline (PRIMARY SOURCE - overrides duplicates)
        'Performance CrdY': 'yellow_cards',
        'Performance CrdR': 'red_cards',
    },
    
    # ===== FROM PLAYER_SHOOTING (UNIQUE STATS ONLY) =====
    'player_shooting': {
        # SKIP: Duplicates from player_standard
        # 'Standard Gls': SKIP - use 'Performance Gls' from standard
        # 'Standard PK': SKIP - use 'Performance PK' from standard  
        # 'Standard PKatt': SKIP - use 'Performance PKatt' from standard
        # 'Expected xG': SKIP - use from standard
        # 'Expected npxG': SKIP - use from standard
        
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
        'Expected npxG/Sh': 'non_penalty_xg_per_shot',
        'Expected G-xG': 'goals_minus_expected_goals',
        'Expected np:G-xG': 'non_penalty_goals_minus_expected',
    },
    
    # ===== FROM PLAYER_PASSING (UNIQUE STATS ONLY) =====
    'player_passing': {
        # SKIP: Duplicates from player_standard
        # 'Ast': SKIP - use 'Performance Ast' from standard
        # 'PrgP': SKIP - use 'Progression PrgP' from standard
        
        # KEEP: Unique passing stats
        'Total Cmp': 'passes_completed',
        'Total Att': 'passes_attempted',
        'Total Cmp%': 'pass_accuracy',
        'Total TotDist': 'total_pass_distance',
        'Total PrgDist': 'progressive_pass_distance',
        'Short Cmp': 'short_passes_completed',
        'Short Att': 'short_passes_attempted',
        'Short Cmp%': 'short_pass_accuracy',
        'Medium Cmp': 'medium_passes_completed',
        'Medium Att': 'medium_passes_attempted',
        'Medium Cmp%': 'medium_pass_accuracy',
        'Long Cmp': 'long_passes_completed',
        'Long Att': 'long_passes_attempted',
        'Long Cmp%': 'long_pass_accuracy',
        'xAG': 'expected_assisted_goals_passing',
        'Expected xA': 'expected_assists',
        'Expected A-xAG': 'assists_minus_expected',
        'KP': 'key_passes',
        '1/3': 'final_third_passes',
        'PPA': 'penalty_area_passes',
        'CrsPA': 'crosses_into_penalty_area',
    },
    
    # ===== FROM PLAYER_PASSINGTYPES (UNIQUE STATS ONLY) =====
    'player_passingtypes': {
        # SKIP: Duplicates from player_passing
        # 'Att': SKIP - use 'Total Att' from passing
        # 'Outcomes Cmp': SKIP - use 'Total Cmp' from passing
        
        # KEEP: Unique pass type stats
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
        'Outcomes Off': 'passes_offside',
        'Outcomes Blocks': 'passes_blocked',
    },
    
    # ===== FROM PLAYER_GOALSHOTCREATION =====
    'player_goalshotcreation': {
        'SCA SCA': 'shot_creating_actions',
        'SCA SCA90': 'shot_creating_actions_per_90',
        'SCA Types PassLive': 'sca_pass_live',
        'SCA Types PassDead': 'sca_pass_dead',
        'SCA Types TO': 'sca_take_ons',
        'SCA Types Sh': 'sca_shots',
        'SCA Types Fld': 'sca_fouls_drawn',
        'SCA Types Def': 'sca_defensive_actions',
        'GCA GCA': 'goal_creating_actions',
        'GCA GCA90': 'goal_creating_actions_per_90',
        'GCA Types PassLive': 'gca_pass_live',
        'GCA Types PassDead': 'gca_pass_dead',
        'GCA Types TO': 'gca_take_ons',
        'GCA Types Sh': 'gca_shots',
        'GCA Types Fld': 'gca_fouls_drawn',
        'GCA Types Def': 'gca_defensive_actions',
    },
    
    # ===== FROM PLAYER_DEFENSE =====
    'player_defense': {
        'Tackles Tkl': 'tackles',
        'Tackles TklW': 'tackles_won',
        'Tackles Def 3rd': 'tackles_def_third',
        'Tackles Mid 3rd': 'tackles_mid_third',
        'Tackles Att 3rd': 'tackles_att_third',
        'Challenges Tkl': 'challenges_attempted',
        'Challenges Att': 'challenges_total',
        'Challenges Tkl%': 'tackle_success_rate',
        'Challenges Lost': 'challenges_lost',
        'Blocks Blocks': 'blocks',
        'Blocks Sh': 'shots_blocked',
        'Blocks Pass': 'passes_blocked_defense',
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

# =====================================================
# GOALKEEPER MAPPINGS  
# Maps raw columns -> analytics_keepers table columns
# =====================================================

GOALKEEPER_MAPPINGS = {
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
        'Launched Cmp': 'long_passes_completed',
        'Launched Att': 'long_passes_attempted',
        'Launched Cmp%': 'long_pass_accuracy',
        'Passes Att (GK)': 'goalkeeper_pass_attempts',
        'Passes Thr': 'throws',
        'Passes Launch%': 'launch_percentage',
        'Passes AvgLen': 'average_pass_length',
        'Goal Kicks Att': 'goal_kick_attempts',
        'Goal Kicks Launch%': 'goal_kick_launch_percentage',
        'Goal Kicks AvgLen': 'goal_kick_average_length',
        'Crosses Opp': 'crosses_faced',
        'Crosses Stp': 'crosses_stopped',
        'Crosses Stp%': 'cross_stopping_percentage',
        'Sweeper #OPA': 'sweeper_actions',
        'Sweeper #OPA/90': 'sweeper_actions_per_90',
        'Sweeper AvgDist': 'sweeper_average_distance',
    },
}

# =====================================================
# COLUMN PRIORITIES (for conflict resolution)
# =====================================================

COLUMN_PRIORITIES = {
    # When same data appears in multiple tables, use this priority order
    'goals': 'player_standard',  # Performance Gls vs Standard Gls
    'assists': 'player_standard',  # Performance Ast vs Ast
    'penalty_kicks_made': 'player_standard',  # Performance PK vs Standard PK
    'penalty_kicks_attempted': 'player_standard',  # Performance PKatt vs Standard PKatt
    'expected_goals': 'player_standard',  # Expected xG appears in standard + shooting
    'non_penalty_expected_goals': 'player_standard',  # Expected npxG appears in standard + shooting
    'yellow_cards': 'player_standard',  # Performance CrdY appears in standard + misc
    'red_cards': 'player_standard',  # Performance CrdR appears in standard + misc
    'progressive_passes': 'player_standard',  # Progression PrgP vs PrgP
    'progressive_carries': 'player_standard',  # Progression PrgC vs Carries PrgC
    'interceptions': 'player_defense',  # Int vs Performance Int
    'tackles_won': 'player_defense',  # Tackles TklW vs Performance TklW
    'passes_completed': 'player_passing',  # Total Cmp vs Outcomes Cmp
    'passes_attempted': 'player_passing',  # Total Att vs Att
    'goals_against': 'player_keepers',  # Performance GA vs Goals GA (for keepers)
}

# =====================================================
# EXCLUDED COLUMNS (always skip these)
# =====================================================

EXCLUDED_COLUMNS = {
    # Metadata columns (handled separately)
    'Current Date',
    'current_through_gameweek', 
    'last_updated',
    
    # Basic info columns (handled in SCD Type 2 framework)
    'Player',
    'Nation', 
    'Pos',
    'Squad',
    'Age',
    'Born',
    
    # 90s column (handled as minutes_90s)
    '90s',
}

# =====================================================
# VALIDATION FUNCTION
# =====================================================

def validate_mappings():
    """Validate that our mappings cover all important columns and have no conflicts"""
    
    # Get all mapped columns for outfield players
    outfield_mapped = set()
    for table_mappings in OUTFIELD_PLAYER_MAPPINGS.values():
        outfield_mapped.update(table_mappings.keys())
    
    # Get all mapped columns for goalkeepers  
    keeper_mapped = set()
    for table_mappings in GOALKEEPER_MAPPINGS.values():
        keeper_mapped.update(table_mappings.keys())
    
    print(f"Outfield columns mapped: {len(outfield_mapped)}")
    print(f"Goalkeeper columns mapped: {len(keeper_mapped)}")
    print(f"Total unique raw columns mapped: {len(outfield_mapped | keeper_mapped)}")
    
    # Check for any analytics column conflicts (same target from different sources)
    outfield_targets = []
    for table_mappings in OUTFIELD_PLAYER_MAPPINGS.values():
        outfield_targets.extend(table_mappings.values())
    
    keeper_targets = []
    for table_mappings in GOALKEEPER_MAPPINGS.values():
        keeper_targets.extend(table_mappings.values())
    
    # Find duplicates
    from collections import Counter
    outfield_duplicates = [col for col, count in Counter(outfield_targets).items() if count > 1]
    keeper_duplicates = [col for col, count in Counter(keeper_targets).items() if count > 1]
    
    if outfield_duplicates:
        print(f"WARNING: Duplicate outfield targets: {outfield_duplicates}")
    if keeper_duplicates:
        print(f"WARNING: Duplicate keeper targets: {keeper_duplicates}")
    
    return len(outfield_mapped), len(keeper_mapped)

# =====================================================
# USAGE EXAMPLE
# =====================================================

def get_analytics_column_name(raw_column: str, table_name: str, player_type: str = 'outfield') -> str:
    """
    Get the analytics column name for a raw column
    
    Args:
        raw_column: Original FBRef column name
        table_name: Source table (e.g., 'player_standard')
        player_type: 'outfield' or 'goalkeeper'
    
    Returns:
        Analytics column name or None if not mapped
    """
    mappings = OUTFIELD_PLAYER_MAPPINGS if player_type == 'outfield' else GOALKEEPER_MAPPINGS
    
    if table_name in mappings:
        return mappings[table_name].get(raw_column)
    
    return None

# Run validation
if __name__ == "__main__":
    validate_mappings()