import streamlit as st
import pandas as pd
import duckdb

st.set_page_config(
    page_title="Team Comparison",
    page_icon="⚽",
    layout="wide"
)

def load_team_list():
    """Load list of teams from database"""
    conn = duckdb.connect("data/premierleague_raw.duckdb")
    
    try:
        query = """
        SELECT DISTINCT Squad as team_name 
        FROM squad_standard 
        WHERE Squad IS NOT NULL 
        ORDER BY Squad
        """
        df = conn.execute(query).fetchdf()
        teams = df['team_name'].tolist()
        conn.close()
        return teams
    except Exception as e:
        conn.close()
        st.error(f"Error loading teams: {e}")
        return []

def calculate_defensive_stats(team_name, conn):
    """Calculate defensive stats from fixtures and squad tables"""
    
    # Goals against and clean sheets from fixtures (UNCHANGED)
    defensive_from_fixtures = conn.execute(f"""
        SELECT 
            COUNT(*) as matches_played,
            SUM(CASE 
                WHEN home_team = '{team_name}' THEN away_score
                WHEN away_team = '{team_name}' THEN home_score
                ELSE 0 
            END) as goals_against,
            SUM(CASE 
                WHEN (home_team = '{team_name}' AND away_score = 0) OR 
                     (away_team = '{team_name}' AND home_score = 0) 
                THEN 1 ELSE 0 
            END) as clean_sheets
        FROM raw_fixtures 
        WHERE (home_team = '{team_name}' OR away_team = '{team_name}')
        AND is_completed = true
    """).fetchone()
    
    matches = defensive_from_fixtures[0] if defensive_from_fixtures[0] else 1
    goals_against = defensive_from_fixtures[1] if defensive_from_fixtures[1] else 0
    clean_sheets = defensive_from_fixtures[2] if defensive_from_fixtures[2] else 0
    clean_sheet_pct = (clean_sheets / matches * 100) if matches > 0 else 0
    
    # EXPANDED: Calculate opponent performance (what team allows) - use vs format
    vs_team_name = f"vs {team_name}"
    opponent_performance = conn.execute(f"""
        SELECT 
            -- Existing attacking stats
            o."Expected xG" as xg_against,
            o."Expected npxG" as npxg_against,
            os."Standard Sh" as shots_against,
            os."Standard SoT" as shots_on_target_against,
            
            -- NEW: Opponent Passing Performance (what team allows)
            op."Total Cmp%" as opponent_pass_accuracy,
            op.PrgP as opponent_progressive_passes,
            op.KP as opponent_key_passes,
            op."1/3" as opponent_final_third_passes,
            op.PPA as opponent_penalty_area_passes,
            
            -- NEW: Opponent Possession Performance (what team allows)
            opos."Take-Ons Succ%" as opponent_takeon_success_rate,
            opos."Take-Ons Succ" as opponent_successful_takerons,
            opos."Carries PrgC" as opponent_progressive_carries,
            opos."Touches Att 3rd" as opponent_touches_att_third,
            opos."Touches Att Pen" as opponent_touches_penalty_area,
            opos."Carries 1/3" as opponent_carries_final_third,
            opos."Carries CPA" as opponent_carries_penalty_area,
            
            -- NEW: Opponent Creation Performance (what team allows)
            ogsc."SCA SCA" as opponent_shot_creating_actions,
            ogsc."GCA GCA" as opponent_goal_creating_actions,
            ogsc."SCA SCA90" as opponent_sca_per_90,
            ogsc."GCA GCA90" as opponent_gca_per_90
            
        FROM opponent_standard o
        LEFT JOIN opponent_shooting os ON o.Squad = os.Squad
        LEFT JOIN opponent_passing op ON o.Squad = op.Squad
        LEFT JOIN opponent_possession opos ON o.Squad = opos.Squad  
        LEFT JOIN opponent_goalshotcreation ogsc ON o.Squad = ogsc.Squad
        WHERE o.Squad = '{vs_team_name}'
    """).fetchone()
    
    return {
        'goals_against': goals_against,
        'clean_sheets': clean_sheets,
        'clean_sheet_percentage': clean_sheet_pct,
        
        # Existing stats
        'xg_against': opponent_performance[0] if opponent_performance and opponent_performance[0] else 0,
        'npxg_against': opponent_performance[1] if opponent_performance and opponent_performance[1] else 0,
        'shots_against': opponent_performance[2] if opponent_performance and opponent_performance[2] else 0,
        'shots_on_target_against': opponent_performance[3] if opponent_performance and opponent_performance[3] else 0,
        
        # NEW: Opponent passing disruption
        'opponent_pass_accuracy': opponent_performance[4] if opponent_performance and opponent_performance[4] else 0,
        'opponent_progressive_passes': opponent_performance[5] if opponent_performance and opponent_performance[5] else 0,
        'opponent_key_passes': opponent_performance[6] if opponent_performance and opponent_performance[6] else 0,
        'opponent_final_third_passes': opponent_performance[7] if opponent_performance and opponent_performance[7] else 0,
        'opponent_penalty_area_passes': opponent_performance[8] if opponent_performance and opponent_performance[8] else 0,
        
        # NEW: Opponent possession disruption
        'opponent_takeon_success_rate': opponent_performance[9] if opponent_performance and opponent_performance[9] else 0,
        'opponent_successful_takerons': opponent_performance[10] if opponent_performance and opponent_performance[10] else 0,
        'opponent_progressive_carries': opponent_performance[11] if opponent_performance and opponent_performance[11] else 0,
        'opponent_touches_att_third': opponent_performance[12] if opponent_performance and opponent_performance[12] else 0,
        'opponent_touches_penalty_area': opponent_performance[13] if opponent_performance and opponent_performance[13] else 0,
        'opponent_carries_final_third': opponent_performance[14] if opponent_performance and opponent_performance[14] else 0,
        'opponent_carries_penalty_area': opponent_performance[15] if opponent_performance and opponent_performance[15] else 0,
        
        # NEW: Opponent creation disruption  
        'opponent_shot_creating_actions': opponent_performance[16] if opponent_performance and opponent_performance[16] else 0,
        'opponent_goal_creating_actions': opponent_performance[17] if opponent_performance and opponent_performance[17] else 0,
        'opponent_sca_per_90': opponent_performance[18] if opponent_performance and opponent_performance[18] else 0,
        'opponent_gca_per_90': opponent_performance[19] if opponent_performance and opponent_performance[19] else 0,
    }

def load_team_stats(team_name):
    """Load comprehensive stats for a team with corrected mappings"""
    conn = duckdb.connect("data/premierleague_raw.duckdb")
    
    try:
        # CORRECTED: Squad stats with proper table mappings
        squad_query = f"""
        SELECT 
            s.Squad,
            s."Playing Time MP" as matches_played,
            
            -- Standard Stats (UNCHANGED - these are correct)
            s."Performance Gls" as goals_for,
            s."Expected xG" as xg_for,
            s."Expected npxG" as npxg_for,
            s."Performance Ast" as assists,
            s."Expected xAG" as xag,
            s."Performance G+A" as goals_assists,
            s."Performance G-PK" as non_penalty_goals,
            s."Performance PK" as penalties_scored,
            s."Performance PKatt" as penalties_attempted,
            s."Performance CrdY" as yellow_cards,
            s."Performance CrdR" as red_cards,
            
            -- FIXED: Progressive stats from correct tables
            p.PrgP as progressive_passes,                    -- FROM squad_passing, NOT squad_standard
            pos."Carries PrgC" as progressive_carries,       -- FROM squad_possession, NOT squad_standard
            
            -- Shooting Stats (UNCHANGED - these are correct)
            sh."Standard Sh" as shots,
            sh."Standard SoT" as shots_on_target,
            sh."Standard SoT%" as shot_accuracy,
            sh."Standard G/Sh" as goals_per_shot,
            sh."Standard G/SoT" as goals_per_shot_on_target,
            sh."Standard Dist" as avg_shot_distance,
            sh."Standard FK" as free_kick_shots,
            sh."Expected G-xG" as goals_minus_xg,
            sh."Expected np:G-xG" as np_goals_minus_xg,
            
            -- Passing Stats (UNCHANGED - these are correct)
            p."Total Cmp" as passes_completed,
            p."Total Att" as passes_attempted,
            p."Total Cmp%" as pass_accuracy,
            p."Total TotDist" as total_pass_distance,
            p."Total PrgDist" as progressive_pass_distance,
            p."Short Cmp%" as short_pass_accuracy,
            p."Medium Cmp%" as medium_pass_accuracy,
            p."Long Cmp%" as long_pass_accuracy,
            p.KP as key_passes,
            p."1/3" as final_third_passes,
            p.PPA as penalty_area_passes,
            p.CrsPA as crosses_into_penalty_area,
            
            -- Passing Types (UNCHANGED - these are correct)
            pt."Pass Types Live" as live_ball_passes,
            pt."Pass Types Dead" as dead_ball_passes,
            pt."Pass Types FK" as free_kick_passes,
            pt."Pass Types TB" as through_balls,
            pt."Pass Types Sw" as switches,
            pt."Pass Types Crs" as crosses,
            pt."Pass Types TI" as throw_ins,
            pt."Pass Types CK" as corner_kicks,
            pt."Outcomes Off" as passes_offside,
            pt."Outcomes Blocks" as passes_blocked,
            
            -- Goal/Shot Creation (UNCHANGED - these are correct)
            gsc."SCA SCA" as shot_creating_actions,
            gsc."SCA SCA90" as sca_per_90,
            gsc."GCA GCA" as goal_creating_actions,
            gsc."GCA GCA90" as gca_per_90,
            
            -- Possession Stats (UNCHANGED - these are correct)
            pos.Poss as possession_pct,
            pos."Touches Touches" as touches,
            pos."Touches Def 3rd" as touches_def_third,
            pos."Touches Mid 3rd" as touches_mid_third,
            pos."Touches Att 3rd" as touches_att_third,
            pos."Touches Att Pen" as touches_penalty_area,
            pos."Take-Ons Att" as take_ons_attempted,
            pos."Take-Ons Succ" as take_ons_successful,
            pos."Take-Ons Succ%" as take_on_success_rate,
            pos."Carries Carries" as carries,
            pos."Carries TotDist" as carry_distance,
            pos."Carries PrgDist" as progressive_carry_distance,
            pos."Carries 1/3" as carries_final_third,
            pos."Carries CPA" as carries_penalty_area,
            pos."Carries Mis" as miscontrols,
            pos."Carries Dis" as dispossessed,
            
            -- Defense Stats (UNCHANGED - these are correct)
            d."Tackles Tkl" as tackles,
            d."Tackles TklW" as tackles_won,
            d."Tackles Def 3rd" as tackles_def_third,
            d."Tackles Mid 3rd" as tackles_mid_third,
            d."Tackles Att 3rd" as tackles_att_third,
            d."Challenges Tkl%" as tackle_success_rate,
            d."Blocks Blocks" as blocks,
            d."Blocks Sh" as shots_blocked,
            d."Blocks Pass" as passes_blocked_def,
            d.Int as interceptions,
            d."Tkl+Int" as tackles_plus_interceptions,
            d.Clr as clearances,
            d.Err as errors,
            
            -- Basic Goalkeeper Stats (UNCHANGED - these are correct)
            k."Performance Save%" as save_percentage,
            
            -- NEW: Advanced Goalkeeper Stats (ADDED)
            ka."Expected PSxG" as post_shot_xg,
            ka."Expected PSxG+/-" as psxg_performance,
            ka."Crosses Stp%" as cross_stopping_percentage,
            ka."Sweeper #OPA" as sweeper_actions,
            ka."Sweeper #OPA/90" as sweeper_actions_per_90,
            ka."Launched Cmp%" as keeper_pass_accuracy,
            ka."Passes AvgLen" as keeper_avg_pass_length,
            
            -- Discipline & Misc (UNCHANGED - these are correct)
            m."Performance 2CrdY" as second_yellows,
            m."Performance Fls" as fouls_committed,
            m."Performance Fld" as fouls_drawn,
            m."Performance Off" as offsides,
            m."Performance PKwon" as penalties_won,
            m."Performance PKcon" as penalties_conceded,
            m."Performance OG" as own_goals,
            m."Performance Recov" as ball_recoveries,
            m."Aerial Duels Won" as aerial_duels_won,
            m."Aerial Duels Lost" as aerial_duels_lost,
            m."Aerial Duels Won%" as aerial_duel_success_rate,
            
            -- NEW: Team Success Stats (ADDED)
            pt_time."Team Success PPM" as points_per_match,
            pt_time."Team Success +/-" as goal_difference_team_success,
            pt_time."Team Success +/-90" as goal_difference_per_90,
            pt_time."Team Success (xG) xG+/-" as xg_difference_team_success,
            pt_time."Team Success (xG) xG+/-90" as xg_difference_per_90
            
        FROM squad_standard s
        LEFT JOIN squad_shooting sh ON s.Squad = sh.Squad
        LEFT JOIN squad_passing p ON s.Squad = p.Squad
        LEFT JOIN squad_passingtypes pt ON s.Squad = pt.Squad
        LEFT JOIN squad_goalshotcreation gsc ON s.Squad = gsc.Squad
        LEFT JOIN squad_possession pos ON s.Squad = pos.Squad
        LEFT JOIN squad_defense d ON s.Squad = d.Squad
        LEFT JOIN squad_misc m ON s.Squad = m.Squad
        LEFT JOIN squad_keepers k ON s.Squad = k.Squad
        LEFT JOIN squad_keepersadv ka ON s.Squad = ka.Squad          -- NEW: Advanced keeper stats
        LEFT JOIN squad_playingtime pt_time ON s.Squad = pt_time.Squad  -- NEW: Team success stats
        WHERE s.Squad = '{team_name}'
        """
        
        squad_df = conn.execute(squad_query).fetchdf()
        
        if squad_df.empty:
            conn.close()
            return None
        
        # Get basic stats
        stats = squad_df.iloc[0].to_dict()
        
        # FIXED: Calculate defensive stats properly
        defensive_stats = calculate_defensive_stats(team_name, conn)
        stats.update(defensive_stats)
        
        conn.close()
        return stats
        
    except Exception as e:
        conn.close()
        st.error(f"Error loading stats for {team_name}: {e}")
        return None

def calculate_per_game_stats(stats):
    """Convert total stats to per-game"""
    if not stats or stats.get('matches_played', 0) == 0:
        return stats
    
    games = stats['matches_played']
    per_game_stats = stats.copy()
    
    # Convert these stats to per-game (exclude percentages and rates)
    per_game_fields = [
        'goals_for', 'goals_against', 'xg_for', 'xg_against', 'npxg_for', 'npxg_against',
        'assists', 'xag', 'goals_assists', 'non_penalty_goals',
        'penalties_scored', 'penalties_attempted', 'progressive_carries', 'progressive_passes',
        'shots', 'shots_against', 'shots_on_target', 'shots_on_target_against',
        'free_kick_shots', 'passes_completed', 'passes_attempted', 'total_pass_distance',
        'progressive_pass_distance', 'key_passes', 'final_third_passes', 'penalty_area_passes',
        'crosses_into_penalty_area', 'live_ball_passes', 'dead_ball_passes', 'free_kick_passes',
        'through_balls', 'switches', 'crosses', 'throw_ins', 'corner_kicks', 'passes_offside',
        'passes_blocked', 'shot_creating_actions', 'goal_creating_actions', 'touches',
        'touches_def_third', 'touches_mid_third', 'touches_att_third', 'touches_penalty_area',
        'take_ons_attempted', 'take_ons_successful', 'carries', 'carry_distance',
        'progressive_carry_distance', 'carries_final_third', 'carries_penalty_area',
        'miscontrols', 'dispossessed', 'tackles', 'tackles_won', 'tackles_def_third',
        'tackles_mid_third', 'tackles_att_third', 'blocks', 'shots_blocked', 'passes_blocked_def',
        'interceptions', 'tackles_plus_interceptions', 'clearances', 'errors', 'yellow_cards',
        'red_cards', 'second_yellows', 'fouls_committed', 'fouls_drawn', 'offsides',
        'penalties_won', 'penalties_conceded', 'own_goals', 'ball_recoveries',
        'aerial_duels_won', 'aerial_duels_lost', 'clean_sheets',
        'opponent_progressive_passes', 'opponent_key_passes', 'opponent_final_third_passes',
        'opponent_penalty_area_passes', 'opponent_successful_takerons', 'opponent_progressive_carries',
        'opponent_touches_att_third', 'opponent_touches_penalty_area', 'opponent_carries_final_third',
        'opponent_carries_penalty_area', 'opponent_shot_creating_actions', 'opponent_goal_creating_actions'
        # NEW: Advanced stats that should be per-game
        'post_shot_xg', 'sweeper_actions'
    ]
    
    for field in per_game_fields:
        if field in per_game_stats and per_game_stats[field] is not None:
            per_game_stats[field] = round(per_game_stats[field] / games, 2)
    
    return per_game_stats

def format_stat(value, is_percentage=False):
    """Format stat for display"""
    if value is None:
        return "—"
    
    if is_percentage:
        return f"{value:.1f}%"
    
    if isinstance(value, float):
        return f"{value:.2f}"
    
    return str(value)

def create_comparison_section(title, metrics_list, team_a_stats, team_b_stats, team_a, team_b):
    """Create a comparison section with softer colors"""
    
    st.subheader(title)
    
    comparison_data = []
    
    for metric_name, stat_key, higher_is_better, is_percentage in metrics_list:
        val_a = format_stat(team_a_stats.get(stat_key), is_percentage)
        val_b = format_stat(team_b_stats.get(stat_key), is_percentage)
        comparison_data.append([metric_name, val_a, val_b])
    
    df = pd.DataFrame(comparison_data, columns=["Metric", team_a, team_b])
    
    # Soft color styling function
    def style_soft_colors(df, metrics_list):
        def highlight_better(row):
            styles = [''] * len(row)
            
            metric_name = row['Metric']
            val_a = row[team_a]
            val_b = row[team_b]
            
            # Find the higher_is_better setting for this metric
            higher_is_better = True
            for m_name, stat_key, h_better, is_pct in metrics_list:
                if m_name == metric_name:
                    higher_is_better = h_better
                    break
            
            if val_a != "—" and val_b != "—":
                try:
                    # Handle percentages
                    if "%" in str(val_a):
                        num_a = float(str(val_a).replace('%', ''))
                        num_b = float(str(val_b).replace('%', ''))
                    else:
                        num_a = float(val_a)
                        num_b = float(val_b)
                    
                    # Color based on which is better
                    if abs(num_a - num_b) > 0.01:
                        if higher_is_better:
                            if num_a > num_b:
                                styles[1] = 'background-color: #006400'  # Green
                                styles[2] = 'background-color: #8B0000'  # Red
                            else:
                                styles[1] = 'background-color: #8B0000'  # Red
                                styles[2] = 'background-color: #006400'  # Green
                        else:  # Lower is better
                            if num_a < num_b:
                                styles[1] = 'background-color: #006400'  # Green
                                styles[2] = 'background-color: #8B0000'  # Red
                            else:
                                styles[1] = 'background-color: #8B0000'  # Red
                                styles[2] = 'background-color: #006400'  # Green
                except:
                    pass
            
            return styles
        
        return df.style.apply(highlight_better, axis=1)
    
    styled_df = style_soft_colors(df, metrics_list)
    st.dataframe(styled_df, use_container_width=True, hide_index=True)
    
    return df

# Main app
st.title("⚽ Team Comparison")
st.write("Comprehensive head-to-head comparison of Premier League teams")

# Load teams
teams = load_team_list()

if not teams:
    st.error("Could not load team data")
    st.stop()

# Team selectors
col1, col2 = st.columns(2)

with col1:
    team_a = st.selectbox(
        "Select Team A",
        teams,
        index=teams.index("Manchester City") if "Manchester City" in teams else 0
    )

with col2:
    team_b = st.selectbox(
        "Select Team B", 
        teams,
        index=teams.index("Arsenal") if "Arsenal" in teams else 1
    )

# View options
view_mode = st.radio(
    "View stats as:",
    ["Per Game", "Season Total"],
    horizontal=True
)

# Load team data
team_a_stats = load_team_stats(team_a)
team_b_stats = load_team_stats(team_b)

if not team_a_stats or not team_b_stats:
    st.error("Could not load team statistics")
    st.stop()

# Apply per-game calculation if needed
if view_mode == "Per Game":
    team_a_stats = calculate_per_game_stats(team_a_stats)
    team_b_stats = calculate_per_game_stats(team_b_stats)

def get_team_record(team_name):
    """Get team's win/loss/draw record from fixtures"""
    conn = duckdb.connect("data/premierleague_raw.duckdb")
    
    try:
        # Query fixtures to calculate W/L/D record
        fixtures_query = f"""
        SELECT 
            home_team, away_team, home_score, away_score, is_completed
        FROM raw_fixtures 
        WHERE (home_team = '{team_name}' OR away_team = '{team_name}')
        AND is_completed = true
        """
        
        fixtures_df = conn.execute(fixtures_query).fetchdf()
        conn.close()
        
        if fixtures_df.empty:
            return {"wins": 0, "draws": 0, "losses": 0, "points": 0}
        
        wins = 0
        draws = 0 
        losses = 0
        
        for _, row in fixtures_df.iterrows():
            if row['home_team'] == team_name:
                # Team played at home
                if row['home_score'] > row['away_score']:
                    wins += 1
                elif row['home_score'] == row['away_score']:
                    draws += 1
                else:
                    losses += 1
            else:
                # Team played away
                if row['away_score'] > row['home_score']:
                    wins += 1
                elif row['away_score'] == row['home_score']:
                    draws += 1
                else:
                    losses += 1
        
        points = (wins * 3) + draws
        
        return {"wins": wins, "draws": draws, "losses": losses, "points": points}
        
    except Exception as e:
        conn.close()
        return {"wins": 0, "draws": 0, "losses": 0, "points": 0}

# Enhanced team headers section
st.divider()

col1, col2 = st.columns(2)

# Get records for both teams
team_a_record = get_team_record(team_a)
team_b_record = get_team_record(team_b)

with col1:
    st.markdown(f"""
    ### {team_a}
    **Matches:** {team_a_stats.get('matches_played', 0)}  
    **Record:** {team_a_record['wins']}W - {team_a_record['draws']}D - {team_a_record['losses']}L  
    **Points:** {team_a_record['points']}  
    **Goals For:** {team_a_stats.get('goals_for', 0):.1f} | **Against:** {team_a_stats.get('goals_against', 0):.1f}  
    **Goal Difference:** {(team_a_stats.get('goals_for', 0) - team_a_stats.get('goals_against', 0)):.1f}
    """)

with col2:
    st.markdown(f"""
    ### {team_b}
    **Matches:** {team_b_stats.get('matches_played', 0)}  
    **Record:** {team_b_record['wins']}W - {team_b_record['draws']}D - {team_b_record['losses']}L  
    **Points:** {team_b_record['points']}  
    **Goals For:** {team_b_stats.get('goals_for', 0):.1f} | **Against:** {team_b_stats.get('goals_against', 0):.1f}  
    **Goal Difference:** {(team_b_stats.get('goals_for', 0) - team_b_stats.get('goals_against', 0)):.1f}
    """)

st.divider()

# Attacking & Goals Section
attacking_metrics = [
    ("Goals Scored", "goals_for", True, False),
    ("Expected Goals (xG)", "xg_for", True, False),
    ("Non-Penalty xG", "npxg_for", True, False),
    ("Goals - xG", "goals_minus_xg", True, False),
    ("Non-Penalty Goals", "non_penalty_goals", True, False),
    ("Assists", "assists", True, False),
    ("Expected Assists (xAG)", "xag", True, False),
    ("Goals + Assists", "goals_assists", True, False),
    ("Penalties Scored", "penalties_scored", True, False),
    ("Penalties Attempted", "penalties_attempted", True, False),
]

create_comparison_section("⚽ Goals & Assists", attacking_metrics, team_a_stats, team_b_stats, team_a, team_b)

st.divider()

# Shooting Section
shooting_metrics = [
    ("Shots", "shots", True, False),
    ("Shots on Target", "shots_on_target", True, False),
    ("Shot Accuracy %", "shot_accuracy", True, True),
    ("Goals per Shot", "goals_per_shot", True, False),
    ("Goals per Shot on Target", "goals_per_shot_on_target", True, False),
    ("Average Shot Distance", "avg_shot_distance", False, False),
    ("Free Kick Shots", "free_kick_shots", True, False),
    ("Shot Creating Actions", "shot_creating_actions", True, False),
    ("SCA per 90", "sca_per_90", True, False),
    ("Goal Creating Actions", "goal_creating_actions", True, False),
]

create_comparison_section("🎯 Shooting", shooting_metrics, team_a_stats, team_b_stats, team_a, team_b)

st.divider()

# Passing Section
passing_metrics = [
    ("Passes Completed", "passes_completed", True, False),
    ("Passes Attempted", "passes_attempted", True, False),
    ("Pass Accuracy %", "pass_accuracy", True, True),
    ("Short Pass Accuracy %", "short_pass_accuracy", True, True),
    ("Medium Pass Accuracy %", "medium_pass_accuracy", True, True),
    ("Long Pass Accuracy %", "long_pass_accuracy", True, True),
    ("Progressive Passes", "progressive_passes", True, False),
    ("Key Passes", "key_passes", True, False),
    ("Final Third Passes", "final_third_passes", True, False),
    ("Penalty Area Passes", "penalty_area_passes", True, False),
    ("Through Balls", "through_balls", True, False),
    ("Switches", "switches", True, False),
    ("Crosses", "crosses", True, False),
]

create_comparison_section("📊 Passing", passing_metrics, team_a_stats, team_b_stats, team_a, team_b)

st.divider()

# Possession Section
possession_metrics = [
    ("Possession %", "possession_pct", True, True),
    ("Touches", "touches", True, False),
    ("Touches in Penalty Area", "touches_penalty_area", True, False),
    ("Touches in Attacking Third", "touches_att_third", True, False),
    ("Take-Ons Attempted", "take_ons_attempted", True, False),
    ("Take-Ons Successful", "take_ons_successful", True, False),
    ("Take-On Success Rate %", "take_on_success_rate", True, True),
    ("Carries", "carries", True, False),
    ("Progressive Carries", "progressive_carries", True, False),
    ("Carries into Final Third", "carries_final_third", True, False),
    ("Carries into Penalty Area", "carries_penalty_area", True, False),
    ("Miscontrols", "miscontrols", False, False),
    ("Dispossessed", "dispossessed", False, False),
]

create_comparison_section("🏃 Possession & Movement", possession_metrics, team_a_stats, team_b_stats, team_a, team_b)

st.divider()

# FIXED: Defensive Section with corrected metrics
defensive_metrics = [
    ("Goals Conceded", "goals_against", False, False),
    ("xG Against", "xg_against", False, False),
    ("Shots Against", "shots_against", False, False),
    ("Shots on Target Against", "shots_on_target_against", False, False),
    ("Clean Sheets", "clean_sheets", True, False),
    ("Clean Sheet %", "clean_sheet_percentage", True, True),
    ("Save %", "save_percentage", True, True),
    ("Tackles", "tackles", True, False),
    ("Tackles Won", "tackles_won", True, False),
    ("Tackle Success Rate %", "tackle_success_rate", True, True),
    ("Interceptions", "interceptions", True, False),
    ("Blocks", "blocks", True, False),
    ("Clearances", "clearances", True, False),
    ("Errors Leading to Shots", "errors", False, False),
]

create_comparison_section("🛡️ Defensive", defensive_metrics, team_a_stats, team_b_stats, team_a, team_b)

st.divider()

# NEW: Defensive Disruption Section  
defensive_disruption_metrics = [
    ("Opponent Pass Accuracy %", "opponent_pass_accuracy", False, True),
    ("Opponent Progressive Passes", "opponent_progressive_passes", False, False),
    ("Opponent Key Passes", "opponent_key_passes", False, False),
    ("Opponent Final Third Passes", "opponent_final_third_passes", False, False),
    ("Opponent Penalty Area Passes", "opponent_penalty_area_passes", False, False),
    ("Opponent Take-On Success %", "opponent_takeon_success_rate", False, True),
    ("Opponent Successful Take-Ons", "opponent_successful_takerons", False, False),
    ("Opponent Progressive Carries", "opponent_progressive_carries", False, False),
    ("Opponent Touches in Att Third", "opponent_touches_att_third", False, False),
    ("Opponent Touches in Penalty Area", "opponent_touches_penalty_area", False, False),
    ("Opponent Carries into Final Third", "opponent_carries_final_third", False, False),
    ("Opponent Carries into Penalty Area", "opponent_carries_penalty_area", False, False),
    ("Opponent Shot Creating Actions", "opponent_shot_creating_actions", False, False),
    ("Opponent Goal Creating Actions", "opponent_goal_creating_actions", False, False),
    ("Opponent SCA per 90", "opponent_sca_per_90", False, False),
    ("Opponent GCA per 90", "opponent_gca_per_90", False, False),
]

create_comparison_section("🛡️ Defensive Disruption", defensive_disruption_metrics, team_a_stats, team_b_stats, team_a, team_b)

# NEW: Advanced Goalkeeper Section
goalkeeper_metrics = [
    ("Post-Shot xG", "post_shot_xg", True, False),
    ("PSxG Performance", "psxg_performance", True, False),
    ("Cross Stopping %", "cross_stopping_percentage", True, True),
    ("Sweeper Actions", "sweeper_actions", True, False),
    ("Sweeper Actions/90", "sweeper_actions_per_90", True, False),
    ("Keeper Pass Accuracy %", "keeper_pass_accuracy", True, True),
    ("Keeper Avg Pass Length", "keeper_avg_pass_length", True, False),
]

create_comparison_section("🥅 Advanced Goalkeeper", goalkeeper_metrics, team_a_stats, team_b_stats, team_a, team_b)

st.divider()

# Discipline & Physical Section
discipline_metrics = [
    ("Yellow Cards", "yellow_cards", False, False),
    ("Red Cards", "red_cards", False, False),
    ("Fouls Committed", "fouls_committed", False, False),
    ("Fouls Drawn", "fouls_drawn", True, False),
    ("Offsides", "offsides", False, False),
    ("Penalties Won", "penalties_won", True, False),
    ("Penalties Conceded", "penalties_conceded", False, False),
    ("Own Goals", "own_goals", False, False),
    ("Aerial Duels Won", "aerial_duels_won", True, False),
    ("Aerial Duel Success Rate %", "aerial_duel_success_rate", True, True),
    ("Ball Recoveries", "ball_recoveries", True, False),
]

create_comparison_section("⚡ Discipline & Duels", discipline_metrics, team_a_stats, team_b_stats, team_a, team_b)

st.divider()

# NEW: Team Success Section
team_success_metrics = [
    ("Points per Match", "points_per_match", True, False),
    ("Goal Difference (Team Success)", "goal_difference_team_success", True, False),
    ("Goal Difference per 90", "goal_difference_per_90", True, False),
    ("xG Difference (Team Success)", "xg_difference_team_success", True, False),
    ("xG Difference per 90", "xg_difference_per_90", True, False),
]

create_comparison_section("🏆 Team Success Metrics", team_success_metrics, team_a_stats, team_b_stats, team_a, team_b)