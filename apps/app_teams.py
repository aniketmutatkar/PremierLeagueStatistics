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
    
    # Goals against and clean sheets from fixtures
    defensive_from_fixtures = conn.execute("""
        SELECT 
            COUNT(*) as matches_played,
            SUM(CASE 
                WHEN home_team = ? THEN away_score
                WHEN away_team = ? THEN home_score
                ELSE 0 
            END) as goals_against,
            SUM(CASE 
                WHEN (home_team = ? AND away_score = 0) OR 
                     (away_team = ? AND home_score = 0) 
                THEN 1 ELSE 0 
            END) as clean_sheets
        FROM raw_fixtures 
        WHERE (home_team = ? OR away_team = ?)
        AND is_completed = true
    """, [team_name, team_name, team_name, team_name, team_name, team_name]).fetchone()
    
    matches = defensive_from_fixtures[0] if defensive_from_fixtures[0] else 1
    goals_against = defensive_from_fixtures[1] if defensive_from_fixtures[1] else 0
    clean_sheets = defensive_from_fixtures[2] if defensive_from_fixtures[2] else 0
    clean_sheet_pct = (clean_sheets / matches * 100) if matches > 0 else 0
    
    # Calculate opponent performance (what team allows) - use vs format
    vs_team_name = f"vs {team_name}"
    opponent_performance = conn.execute(f"""
        SELECT 
            -- Existing attacking stats
            o."Expected xG" as xg_against,
            o."Expected npxG" as npxg_against,
            os."Standard Sh" as shots_against,
            os."Standard SoT" as shots_on_target_against,
            
            -- Opponent Passing Performance (what team allows)
            op."Total Cmp%" as opponent_pass_accuracy,
            op.PrgP as opponent_progressive_passes,
            op.KP as opponent_key_passes,
            op."1/3" as opponent_final_third_passes,
            op.PPA as opponent_penalty_area_passes,
            op."Total Cmp" as opponent_passes_completed,
            op."Total Att" as opponent_passes_attempted,
            op."Short Cmp%" as opponent_short_pass_accuracy,
            op."Medium Cmp%" as opponent_medium_pass_accuracy,
            op."Long Cmp%" as opponent_long_pass_accuracy,
            op.CrsPA as opponent_crosses_into_penalty_area,
            
            -- Opponent Possession Performance (what team allows)
            opos."Take-Ons Succ%" as opponent_takeon_success_rate,
            opos."Take-Ons Succ" as opponent_successful_takerons,
            opos."Carries PrgC" as opponent_progressive_carries,
            opos."Touches Att 3rd" as opponent_touches_att_third,
            opos."Touches Att Pen" as opponent_touches_penalty_area,
            opos."Carries 1/3" as opponent_carries_final_third,
            opos."Carries CPA" as opponent_carries_penalty_area,
            opos."Take-Ons Att" as opponent_take_ons_attempted,
            opos."Touches Touches" as opponent_touches,
            opos.Poss as opponent_possession_pct,
            
            -- Opponent Creation Performance (what team allows)
            ogsc."SCA SCA" as opponent_shot_creating_actions,
            ogsc."GCA GCA" as opponent_goal_creating_actions,
            ogsc."SCA SCA90" as opponent_sca_per_90,
            ogsc."GCA GCA90" as opponent_gca_per_90
            
        FROM opponent_standard o
        LEFT JOIN opponent_shooting os ON o.Squad = os.Squad
        LEFT JOIN opponent_passing op ON o.Squad = op.Squad
        LEFT JOIN opponent_possession opos ON o.Squad = opos.Squad  
        LEFT JOIN opponent_goalshotcreation ogsc ON o.Squad = ogsc.Squad
        WHERE o.Squad = ?
    """, [vs_team_name]).fetchone()
    
    return {
        'goals_against': goals_against,
        'clean_sheets': clean_sheets,
        'clean_sheet_percentage': clean_sheet_pct,
        
        # Existing stats
        'xg_against': opponent_performance[0] if opponent_performance and opponent_performance[0] else 0,
        'npxg_against': opponent_performance[1] if opponent_performance and opponent_performance[1] else 0,
        'shots_against': opponent_performance[2] if opponent_performance and opponent_performance[2] else 0,
        'shots_on_target_against': opponent_performance[3] if opponent_performance and opponent_performance[3] else 0,
        
        # Opponent passing disruption
        'opponent_pass_accuracy': opponent_performance[4] if opponent_performance and opponent_performance[4] else 0,
        'opponent_progressive_passes': opponent_performance[5] if opponent_performance and opponent_performance[5] else 0,
        'opponent_key_passes': opponent_performance[6] if opponent_performance and opponent_performance[6] else 0,
        'opponent_final_third_passes': opponent_performance[7] if opponent_performance and opponent_performance[7] else 0,
        'opponent_penalty_area_passes': opponent_performance[8] if opponent_performance and opponent_performance[8] else 0,
        'opponent_passes_completed': opponent_performance[9] if opponent_performance and opponent_performance[9] else 0,
        'opponent_passes_attempted': opponent_performance[10] if opponent_performance and opponent_performance[10] else 0,
        'opponent_short_pass_accuracy': opponent_performance[11] if opponent_performance and opponent_performance[11] else 0,
        'opponent_medium_pass_accuracy': opponent_performance[12] if opponent_performance and opponent_performance[12] else 0,
        'opponent_long_pass_accuracy': opponent_performance[13] if opponent_performance and opponent_performance[13] else 0,
        'opponent_crosses_into_penalty_area': opponent_performance[14] if opponent_performance and opponent_performance[14] else 0,
        
        # Opponent possession disruption
        'opponent_takeon_success_rate': opponent_performance[15] if opponent_performance and opponent_performance[15] else 0,
        'opponent_successful_takerons': opponent_performance[16] if opponent_performance and opponent_performance[16] else 0,
        'opponent_progressive_carries': opponent_performance[17] if opponent_performance and opponent_performance[17] else 0,
        'opponent_touches_att_third': opponent_performance[18] if opponent_performance and opponent_performance[18] else 0,
        'opponent_touches_penalty_area': opponent_performance[19] if opponent_performance and opponent_performance[19] else 0,
        'opponent_carries_final_third': opponent_performance[20] if opponent_performance and opponent_performance[20] else 0,
        'opponent_carries_penalty_area': opponent_performance[21] if opponent_performance and opponent_performance[21] else 0,
        'opponent_take_ons_attempted': opponent_performance[22] if opponent_performance and opponent_performance[22] else 0,
        'opponent_touches': opponent_performance[23] if opponent_performance and opponent_performance[23] else 0,
        'opponent_possession_pct': opponent_performance[24] if opponent_performance and opponent_performance[24] else 0,
        
        # Opponent creation disruption  
        'opponent_shot_creating_actions': opponent_performance[25] if opponent_performance and opponent_performance[25] else 0,
        'opponent_goal_creating_actions': opponent_performance[26] if opponent_performance and opponent_performance[26] else 0,
        'opponent_sca_per_90': opponent_performance[27] if opponent_performance and opponent_performance[27] else 0,
        'opponent_gca_per_90': opponent_performance[28] if opponent_performance and opponent_performance[28] else 0,
    }

def load_team_stats(team_name):
    """Load comprehensive stats for a team with corrected mappings"""
    conn = duckdb.connect("data/premierleague_raw.duckdb")
    
    try:
        # Squad stats with proper table mappings
        squad_query = f"""
        SELECT 
            s.Squad,
            s."Playing Time MP" as matches_played,
            
            -- Standard Stats
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
            
            -- Progressive stats from correct tables
            p.PrgP as progressive_passes,
            pos."Carries PrgC" as progressive_carries,
            
            -- Shooting Stats
            sh."Standard Sh" as shots,
            sh."Standard SoT" as shots_on_target,
            sh."Standard SoT%" as shot_accuracy,
            sh."Standard G/Sh" as goals_per_shot,
            sh."Standard G/SoT" as goals_per_shot_on_target,
            sh."Standard Dist" as avg_shot_distance,
            sh."Standard FK" as free_kick_shots,
            sh."Expected G-xG" as goals_minus_xg,
            sh."Expected np:G-xG" as np_goals_minus_xg,
            
            -- Passing Stats
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
            
            -- Passing Types
            pt."Pass Types Live" as live_ball_passes,
            pt."Pass Types Dead" as dead_ball_passes,
            pt."Pass Types TB" as through_balls,
            pt."Pass Types Sw" as switches,
            pt."Pass Types Crs" as crosses,
            pt."Pass Types TI" as throw_ins,
            pt."Pass Types CK" as corner_kicks,
            
            -- Goal/Shot Creation
            gsc."SCA SCA" as shot_creating_actions,
            gsc."SCA SCA90" as sca_per_90,
            gsc."GCA GCA" as goal_creating_actions,
            gsc."GCA GCA90" as gca_per_90,
            
            -- Possession Stats
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
            
            -- Defense Stats
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
            
            -- Goalkeeper Stats
            k."Performance Save%" as save_percentage,
            ka."Expected PSxG" as post_shot_xg,
            ka."Expected PSxG+/-" as psxg_performance,
            ka."Crosses Stp%" as cross_stopping_percentage,
            ka."Sweeper #OPA" as sweeper_actions,
            ka."Sweeper #OPA/90" as sweeper_actions_per_90,
            ka."Launched Cmp%" as keeper_pass_accuracy,
            ka."Passes AvgLen" as keeper_avg_pass_length,
            
            -- Discipline & Misc
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
            
            -- Team Success Stats
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
        LEFT JOIN squad_keepersadv ka ON s.Squad = ka.Squad
        LEFT JOIN squad_playingtime pt_time ON s.Squad = pt_time.Squad
        WHERE s.Squad = ?
        """
        
        squad_df = conn.execute(squad_query, [team_name]).fetchdf()
        
        if squad_df.empty:
            conn.close()
            return None
        
        # Get basic stats
        stats = squad_df.iloc[0].to_dict()
        
        # Calculate defensive stats properly
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
        'assists', 'xag', 'goals_assists', 'non_penalty_goals', 'penalties_scored', 'penalties_attempted',
        'progressive_carries', 'progressive_passes', 'shots', 'shots_against', 'shots_on_target', 
        'shots_on_target_against', 'free_kick_shots', 'passes_completed', 'passes_attempted',
        'total_pass_distance', 'progressive_pass_distance', 'key_passes', 'final_third_passes', 
        'penalty_area_passes', 'crosses_into_penalty_area', 'live_ball_passes', 'dead_ball_passes',
        'through_balls', 'switches', 'crosses', 'throw_ins', 'corner_kicks', 'shot_creating_actions',
        'goal_creating_actions', 'touches', 'touches_def_third', 'touches_mid_third', 'touches_att_third',
        'touches_penalty_area', 'take_ons_attempted', 'take_ons_successful', 'carries', 'carry_distance',
        'progressive_carry_distance', 'carries_final_third', 'carries_penalty_area', 'miscontrols',
        'dispossessed', 'tackles', 'tackles_won', 'tackles_def_third', 'tackles_mid_third',
        'tackles_att_third', 'blocks', 'shots_blocked', 'passes_blocked_def', 'interceptions',
        'tackles_plus_interceptions', 'clearances', 'errors', 'yellow_cards', 'red_cards',
        'second_yellows', 'fouls_committed', 'fouls_drawn', 'offsides', 'penalties_won',
        'penalties_conceded', 'own_goals', 'ball_recoveries', 'aerial_duels_won', 'aerial_duels_lost',
        'clean_sheets', 'post_shot_xg', 'sweeper_actions', 'opponent_progressive_passes',
        'opponent_key_passes', 'opponent_final_third_passes', 'opponent_penalty_area_passes',
        'opponent_successful_takerons', 'opponent_progressive_carries', 'opponent_touches_att_third',
        'opponent_touches_penalty_area', 'opponent_carries_final_third', 'opponent_carries_penalty_area',
        'opponent_shot_creating_actions', 'opponent_goal_creating_actions', 'opponent_passes_completed',
        'opponent_passes_attempted', 'opponent_take_ons_attempted', 'opponent_touches',
        'opponent_crosses_into_penalty_area'
    ]
    
    for field in per_game_fields:
        if field in per_game_stats and per_game_stats[field] is not None:
            per_game_stats[field] = round(per_game_stats[field] / games, 2)
    
    return per_game_stats

def get_team_record(team_name):
    """Get team's win/loss/draw record from fixtures"""
    conn = duckdb.connect("data/premierleague_raw.duckdb")
    
    try:
        fixtures_query = """
        SELECT 
            home_team, away_team, home_score, away_score, is_completed
        FROM raw_fixtures 
        WHERE (home_team = ? OR away_team = ?)
        AND is_completed = true
        """

        fixtures_df = conn.execute(fixtures_query, [team_name, team_name]).fetchdf()
        conn.close()
        
        if fixtures_df.empty:
            return {"wins": 0, "draws": 0, "losses": 0, "points": 0}
        
        wins = 0
        draws = 0 
        losses = 0
        
        for _, row in fixtures_df.iterrows():
            if row['home_team'] == team_name:
                if row['home_score'] > row['away_score']:
                    wins += 1
                elif row['home_score'] == row['away_score']:
                    draws += 1
                else:
                    losses += 1
            else:
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

def format_stat(value, is_percentage=False):
    """Format stat for display"""
    if value is None:
        return "—"
    
    if is_percentage:
        return f"{value:.1f}%"
    
    if isinstance(value, float):
        return f"{value:.2f}"
    
    return str(value)

def create_grouped_comparison_table(group_title, metrics_list, team_a_stats, team_b_stats, team_a, team_b):
    """Create a grouped comparison table with better organization"""
    
    st.markdown(f"**{group_title}**")
    
    comparison_data = []
    
    for metric_name, stat_key, higher_is_better, is_percentage in metrics_list:
        val_a = format_stat(team_a_stats.get(stat_key), is_percentage)
        val_b = format_stat(team_b_stats.get(stat_key), is_percentage)
        comparison_data.append([metric_name, val_a, val_b])
    
    df = pd.DataFrame(comparison_data, columns=["Metric", team_a, team_b])
    
    # Color styling function
    def style_colors(df, metrics_list):
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
                                styles[1] = 'background-color: #006400' 
                                styles[2] = 'background-color: #8B0000'
                            else:
                                styles[1] = 'background-color: #8B0000'
                                styles[2] = 'background-color: #006400' 
                        else:  # Lower is better
                            if num_a < num_b:
                                styles[1] = 'background-color: #006400' 
                                styles[2] = 'background-color: #8B0000'
                            else:
                                styles[1] = 'background-color: #8B0000'
                                styles[2] = 'background-color: #006400' 
                except:
                    pass
            
            return styles
        
        return df.style.apply(highlight_better, axis=1)
    
    styled_df = style_colors(df, metrics_list)
    st.dataframe(styled_df, use_container_width=True, hide_index=True)

def create_grouped_opponent_table(group_title, metrics_list, team_a_stats, team_b_stats, team_a, team_b):
    """Create a grouped opponent comparison table"""
    
    st.markdown(f"**{group_title}**")
    
    comparison_data = []
    
    for metric_name, stat_key, higher_is_better, is_percentage in metrics_list:
        val_a = format_stat(team_a_stats.get(stat_key), is_percentage)
        val_b = format_stat(team_b_stats.get(stat_key), is_percentage)
        comparison_data.append([metric_name, val_a, val_b])
    
    df = pd.DataFrame(comparison_data, columns=["Metric", f"vs {team_a}", f"vs {team_b}"])
    
    # Color styling function (same logic but for opponent stats)
    def style_colors(df, metrics_list):
        def highlight_better(row):
            styles = [''] * len(row)
            
            metric_name = row['Metric']
            val_a = row[f"vs {team_a}"]
            val_b = row[f"vs {team_b}"]
            
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
                    
                    # Color based on which is better (note: for opponent stats, logic is inverted)
                    if abs(num_a - num_b) > 0.01:
                        if higher_is_better:
                            if num_a > num_b:
                                styles[1] = 'background-color: #8B0000'# Red (worse for team A)
                                styles[2] = 'background-color: #006400' # Green (better for team B)
                            else:
                                styles[1] = 'background-color: #006400' # Green (better for team A)
                                styles[2] = 'background-color: #8B0000'# Red (worse for team B)
                        else:  # Lower is better
                            if num_a < num_b:
                                styles[1] = 'background-color: #006400' # Green (better for team A)
                                styles[2] = 'background-color: #8B0000'# Red (worse for team B)
                            else:
                                styles[1] = 'background-color: #8B0000'# Red (worse for team A)
                                styles[2] = 'background-color: #006400' # Green (better for team B)
                except:
                    pass
            
            return styles
        
        return df.style.apply(highlight_better, axis=1)
    
    styled_df = style_colors(df, metrics_list)
    st.dataframe(styled_df, use_container_width=True, hide_index=True)

# Main app
st.title("Premier League Team Comparison")

# Center the team selection 
_, center_col, _ = st.columns([1.5, 3, 1])

with center_col:
    col1, col2, col3 = st.columns([3, 3, 2])

    # Load teams
    teams = load_team_list()
    if not teams:
        st.error("Could not load team data")
        st.stop()

    with col1:
        team_a = st.selectbox(
            "Team A",
            teams,
            index=teams.index("Manchester City") if "Manchester City" in teams else 0
        )

    with col2:
        team_b = st.selectbox(
            "Team B", 
            teams,
            index=teams.index("Arsenal") if "Arsenal" in teams else 1
        )

    with col3:
        view_mode = st.radio("View", ["Per Game", "Season Total"], horizontal=True)

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

# Get records
team_a_record = get_team_record(team_a)
team_b_record = get_team_record(team_b)

# Create team cards using info boxes - centered layout
_, card_center, _= st.columns([1, 10, 1])

with card_center:
    # Three columns: Team A card, VS, Team B card
    team_a_col, _, vs_col, team_b_col = st.columns([2, 0.5, 1, 2])
    
    with team_a_col:
        # Team A info card
        goal_diff_a = team_a_stats.get('goals_for', 0) - team_a_stats.get('goals_against', 0)
        team_a_card = f"""
# {team_a}

**Record:** {team_a_record['wins']}-{team_a_record['draws']}-{team_a_record['losses']}  
**Points:** {team_a_record['points']}  
**Goal Difference:** {goal_diff_a:+.1f}  
**Goals:** {team_a_stats.get('goals_for', 0):.1f} for, {team_a_stats.get('goals_against', 0):.1f} against
        """
        st.info(team_a_card)
    
    with vs_col:
        st.markdown("")
        st.markdown("")
        st.markdown("## **VS**")
    
    with team_b_col:
        # Team B info card
        goal_diff_b = team_b_stats.get('goals_for', 0) - team_b_stats.get('goals_against', 0)
        team_b_card = f"""
# {team_b}

**Record:** {team_b_record['wins']}-{team_b_record['draws']}-{team_b_record['losses']}  
**Points:** {team_b_record['points']}  
**Goal Difference:** {goal_diff_b:+.1f}  
**Goals:** {team_b_stats.get('goals_for', 0):.1f} for, {team_b_stats.get('goals_against', 0):.1f} against
        """
        st.info(team_b_card)

st.markdown("---")

# TWO-COLUMN LAYOUT
col_left, col_right = st.columns(2)

with col_left:
    st.header(f"Direct Comparison: {team_a} vs {team_b}")
    
    # Use tabs for organization within left column
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["Attack", "Passing", "Defense", "Discipline", "Advanced"])
    
    with tab1:
        # Finishing Group
        finishing_metrics = [
            ("Goals", "goals_for", True, False),
            ("Expected Goals (xG)", "xg_for", True, False),
            ("Non-Penalty xG", "npxg_for", True, False),
            ("Goals - xG", "goals_minus_xg", True, False),
            ("Non-Penalty Goals", "non_penalty_goals", True, False),
            ("Goals per Shot", "goals_per_shot", True, False),
            ("Goals per Shot on Target", "goals_per_shot_on_target", True, False),
        ]
        create_grouped_comparison_table("Finishing", finishing_metrics, team_a_stats, team_b_stats, team_a, team_b)
        
        # Chance Creation Group
        chance_creation_metrics = [
            ("Shots", "shots", True, False),
            ("Shots on Target", "shots_on_target", True, False),
            ("Shot Accuracy %", "shot_accuracy", True, True),
            ("Average Shot Distance", "avg_shot_distance", False, False),
            ("Free Kick Shots", "free_kick_shots", True, False),
        ]
        create_grouped_comparison_table("Chance Creation", chance_creation_metrics, team_a_stats, team_b_stats, team_a, team_b)
        
        # Playmaking Group
        playmaking_metrics = [
            ("Assists", "assists", True, False),
            ("Expected Assists (xAG)", "xag", True, False),
            ("Goals + Assists", "goals_assists", True, False),
            ("SCA", "shot_creating_actions", True, False),
            ("SCA per 90", "sca_per_90", True, False),
            ("GCA", "goal_creating_actions", True, False),
            ("GCA per 90", "gca_per_90", True, False),
        ]
        create_grouped_comparison_table("Playmaking", playmaking_metrics, team_a_stats, team_b_stats, team_a, team_b)
        
        # Set Pieces Group
        set_pieces_metrics = [
            ("Penalties Scored", "penalties_scored", True, False),
            ("Penalties Attempted", "penalties_attempted", True, False),
        ]
        create_grouped_comparison_table("Set Pieces", set_pieces_metrics, team_a_stats, team_b_stats, team_a, team_b)
    
    with tab2:
        # Passing Accuracy Group
        passing_accuracy_metrics = [
            ("Passes Completed", "passes_completed", True, False),
            ("Passes Attempted", "passes_attempted", True, False),
            ("Pass Accuracy %", "pass_accuracy", True, True),
            ("Short Pass Accuracy %", "short_pass_accuracy", True, True),
            ("Medium Pass Accuracy %", "medium_pass_accuracy", True, True),
            ("Long Pass Accuracy %", "long_pass_accuracy", True, True),
        ]
        create_grouped_comparison_table("Passing Accuracy", passing_accuracy_metrics, team_a_stats, team_b_stats, team_a, team_b)
        
        # Progressive Passing Group
        progressive_passing_metrics = [
            ("Progressive Passes", "progressive_passes", True, False),
            ("Progressive Pass Distance", "progressive_pass_distance", True, False),
            ("Key Passes", "key_passes", True, False),
            ("Final Third Passes", "final_third_passes", True, False),
            ("Penalty Area Passes", "penalty_area_passes", True, False),
            ("Crosses into Penalty Area", "crosses_into_penalty_area", True, False),
        ]
        create_grouped_comparison_table("Progressive Passing", progressive_passing_metrics, team_a_stats, team_b_stats, team_a, team_b)
        
        # Pass Types Group
        pass_types_metrics = [
            ("Through Balls", "through_balls", True, False),
            ("Switches", "switches", True, False),
            ("Crosses", "crosses", True, False),
            ("Live Ball Passes", "live_ball_passes", True, False),
            ("Dead Ball Passes", "dead_ball_passes", True, False),
        ]
        create_grouped_comparison_table("Pass Types", pass_types_metrics, team_a_stats, team_b_stats, team_a, team_b)
        
        # Set Pieces Group
        set_piece_passing_metrics = [
            ("Throw-ins", "throw_ins", True, False),
            ("Corner Kicks", "corner_kicks", True, False),
        ]
        create_grouped_comparison_table("Set Piece Delivery", set_piece_passing_metrics, team_a_stats, team_b_stats, team_a, team_b)
    
    with tab3:
        # Goals Against Group
        goals_against_metrics = [
            ("Goals Against", "goals_against", False, False),
            ("xG Against", "xg_against", False, False),
            ("Shots Against", "shots_against", False, False),
            ("Shots on Target Against", "shots_on_target_against", False, False),
            ("Clean Sheets", "clean_sheets", True, False),
            ("Clean Sheet %", "clean_sheet_percentage", True, True),
        ]
        create_grouped_comparison_table("Goals Against", goals_against_metrics, team_a_stats, team_b_stats, team_a, team_b)
        
        # Defensive Actions Group
        defensive_actions_metrics = [
            ("Tackles", "tackles", True, False),
            ("Tackles Won", "tackles_won", True, False),
            ("Tackle Success %", "tackle_success_rate", True, True),
            ("Interceptions", "interceptions", True, False),
            ("Tackles + Interceptions", "tackles_plus_interceptions", True, False),
            ("Clearances", "clearances", True, False),
        ]
        create_grouped_comparison_table("Defensive Actions", defensive_actions_metrics, team_a_stats, team_b_stats, team_a, team_b)
        
        # Defensive Areas Group
        defensive_areas_metrics = [
            ("Tackles Def Third", "tackles_def_third", True, False),
            ("Tackles Mid Third", "tackles_mid_third", True, False),
            ("Tackles Att Third", "tackles_att_third", True, False),
        ]
        create_grouped_comparison_table("Defensive Areas", defensive_areas_metrics, team_a_stats, team_b_stats, team_a, team_b)
        
        # Blocks & Errors Group
        blocks_errors_metrics = [
            ("Blocks", "blocks", True, False),
            ("Shots Blocked", "shots_blocked", True, False),
            ("Passes Blocked", "passes_blocked_def", True, False),
            ("Errors Leading to Shots", "errors", False, False),
        ]
        create_grouped_comparison_table("Blocks & Errors", blocks_errors_metrics, team_a_stats, team_b_stats, team_a, team_b)
        
        # Goalkeeping Group
        goalkeeping_metrics = [
            ("Save %", "save_percentage", True, True),
        ]
        create_grouped_comparison_table("Goalkeeping", goalkeeping_metrics, team_a_stats, team_b_stats, team_a, team_b)
    
    with tab4:
        # Cards & Discipline Group
        discipline_metrics = [
            ("Yellow Cards", "yellow_cards", False, False),
            ("Red Cards", "red_cards", False, False),
            ("Second Yellow Cards", "second_yellows", False, False),
        ]
        create_grouped_comparison_table("Cards & Discipline", discipline_metrics, team_a_stats, team_b_stats, team_a, team_b)
        
        # Fouls Group
        fouls_metrics = [
            ("Fouls Committed", "fouls_committed", False, False),
            ("Fouls Drawn", "fouls_drawn", True, False),
            ("Offsides", "offsides", False, False),
        ]
        create_grouped_comparison_table("Fouls", fouls_metrics, team_a_stats, team_b_stats, team_a, team_b)
        
        # Penalties & Own Goals Group
        penalties_og_metrics = [
            ("Penalties Won", "penalties_won", True, False),
            ("Penalties Conceded", "penalties_conceded", False, False),
            ("Own Goals", "own_goals", False, False),
        ]
        create_grouped_comparison_table("Penalties & Own Goals", penalties_og_metrics, team_a_stats, team_b_stats, team_a, team_b)
        
        # Physical Duels Group
        physical_metrics = [
            ("Ball Recoveries", "ball_recoveries", True, False),
            ("Aerial Duels Won", "aerial_duels_won", True, False),
            ("Aerial Duels Lost", "aerial_duels_lost", False, False),
            ("Aerial Win %", "aerial_duel_success_rate", True, True),
        ]
        create_grouped_comparison_table("Physical Duels", physical_metrics, team_a_stats, team_b_stats, team_a, team_b)
    
    with tab5:
        # Possession Control Group
        possession_control_metrics = [
            ("Possession %", "possession_pct", True, True),
            ("Touches", "touches", True, False),
            ("Touches Def Third", "touches_def_third", True, False),
            ("Touches Mid Third", "touches_mid_third", True, False),
            ("Touches Att Third", "touches_att_third", True, False),
            ("Touches Penalty Area", "touches_penalty_area", True, False),
        ]
        create_grouped_comparison_table("Possession Control", possession_control_metrics, team_a_stats, team_b_stats, team_a, team_b)
        
        # Ball Carrying Group
        ball_carrying_metrics = [
            ("Progressive Carries", "progressive_carries", True, False),
            ("Progressive Carry Distance", "progressive_carry_distance", True, False),
            ("Carries", "carries", True, False),
            ("Carries Final Third", "carries_final_third", True, False),
            ("Carries Penalty Area", "carries_penalty_area", True, False),
        ]
        create_grouped_comparison_table("Ball Carrying", ball_carrying_metrics, team_a_stats, team_b_stats, team_a, team_b)
        
        # Take-Ons Group
        takerons_metrics = [
            ("Take-Ons Attempted", "take_ons_attempted", True, False),
            ("Take-Ons Successful", "take_ons_successful", True, False),
            ("Take-On Success %", "take_on_success_rate", True, True),
            ("Miscontrols", "miscontrols", False, False),
            ("Dispossessed", "dispossessed", False, False),
        ]
        create_grouped_comparison_table("Take-Ons & Ball Control", takerons_metrics, team_a_stats, team_b_stats, team_a, team_b)
        
        # Advanced Goalkeeper Stats
        goalkeeper_advanced_metrics = [
            ("Post-Shot xG", "post_shot_xg", False, False),  # Higher = faced harder shots
            ("PSxG Performance", "psxg_performance", True, False),
            ("Cross Stopping %", "cross_stopping_percentage", True, True),
            ("Sweeper Actions", "sweeper_actions", True, False),
            ("Sweeper Actions/90", "sweeper_actions_per_90", True, False),
            ("Keeper Pass Accuracy %", "keeper_pass_accuracy", True, True),
            ("Keeper Avg Pass Length", "keeper_avg_pass_length", True, False),
        ]
        create_grouped_comparison_table("Advanced Goalkeeper", goalkeeper_advanced_metrics, team_a_stats, team_b_stats, team_a, team_b)
        
        # Team Success Stats
        team_success_metrics = [
            ("Points per Match", "points_per_match", True, False),
            ("Goal Difference (Team Success)", "goal_difference_team_success", True, False),
            ("Goal Difference per 90", "goal_difference_per_90", True, False),
            ("xG Difference (Team Success)", "xg_difference_team_success", True, False),
            ("xG Difference per 90", "xg_difference_per_90", True, False),
        ]
        create_grouped_comparison_table("Team Success Metrics", team_success_metrics, team_a_stats, team_b_stats, team_a, team_b)

with col_right:
    st.header("Defensive Impact Analysis")
    st.caption("How opponents perform when facing each team")
    
    # Add explanation
    st.info("**What this shows:** How opposing teams perform when they face each of these teams. Lower values typically indicate better defensive performance by the team being analyzed.")
    
    # Use tabs for organization within right column
    tab1, tab2, tab3, tab4 = st.tabs(["Attack Allowed", "Passing Allowed", "Possession Allowed", "Creation Allowed"])
    
    with tab1:
        # What opponents score
        opponent_scoring_metrics = [
            ("Goals Scored vs Team", "goals_against", False, False),  # Lower is better for defending team
            ("xG vs Team", "xg_against", False, False),
            ("npxG vs Team", "npxg_against", False, False),
        ]
        create_grouped_opponent_table("Opponent Scoring", opponent_scoring_metrics, team_a_stats, team_b_stats, team_a, team_b)
        
        # What opponents shoot
        opponent_shooting_metrics = [
            ("Shots vs Team", "shots_against", False, False),
            ("Shots on Target vs Team", "shots_on_target_against", False, False),
        ]
        create_grouped_opponent_table("Opponent Shooting", opponent_shooting_metrics, team_a_stats, team_b_stats, team_a, team_b)
        
        st.markdown("**Lower values = Better defense**")
        st.caption("Shows what opponents typically achieve when playing against each team")
    
    with tab2:
        # Opponent passing volume
        opponent_passing_volume_metrics = [
            ("Passes Completed vs Team", "opponent_passes_completed", False, False),
            ("Passes Attempted vs Team", "opponent_passes_attempted", False, False),
        ]
        create_grouped_opponent_table("Opponent Passing Volume", opponent_passing_volume_metrics, team_a_stats, team_b_stats, team_a, team_b)
        
        # Opponent passing accuracy
        opponent_passing_accuracy_metrics = [
            ("Pass Accuracy vs Team %", "opponent_pass_accuracy", False, True),  # Lower is better for defending team
            ("Short Pass Accuracy vs Team %", "opponent_short_pass_accuracy", False, True),
            ("Medium Pass Accuracy vs Team %", "opponent_medium_pass_accuracy", False, True),
            ("Long Pass Accuracy vs Team %", "opponent_long_pass_accuracy", False, True),
        ]
        create_grouped_opponent_table("Opponent Passing Accuracy", opponent_passing_accuracy_metrics, team_a_stats, team_b_stats, team_a, team_b)
        
        # Opponent progressive passing
        opponent_progressive_passing_metrics = [
            ("Progressive Passes vs Team", "opponent_progressive_passes", False, False),
            ("Key Passes vs Team", "opponent_key_passes", False, False),
            ("Final Third Passes vs Team", "opponent_final_third_passes", False, False),
            ("Penalty Area Passes vs Team", "opponent_penalty_area_passes", False, False),
            ("Crosses into Pen Area vs Team", "opponent_crosses_into_penalty_area", False, False),
        ]
        create_grouped_opponent_table("Opponent Progressive Passing", opponent_progressive_passing_metrics, team_a_stats, team_b_stats, team_a, team_b)
        
        st.markdown("**Lower values = Better defensive disruption**")
        st.caption("Shows how accurately/effectively opponents pass when facing each team")
    
    with tab3:
        # Opponent ball control
        opponent_control_metrics = [
            ("Opponent Possession % vs Team", "opponent_possession_pct", False, True),
            ("Opponent Touches vs Team", "opponent_touches", False, False),
            ("Opponent Touches Att Third vs Team", "opponent_touches_att_third", False, False),
            ("Opponent Touches Penalty Area vs Team", "opponent_touches_penalty_area", False, False),
        ]
        create_grouped_opponent_table("Opponent Ball Control", opponent_control_metrics, team_a_stats, team_b_stats, team_a, team_b)
        
        # Opponent movement
        opponent_movement_metrics = [
            ("Take-Ons Attempted vs Team", "opponent_take_ons_attempted", False, False),
            ("Take-On Success vs Team %", "opponent_takeon_success_rate", False, True),  # Lower is better for defending team
            ("Successful Take-Ons vs Team", "opponent_successful_takerons", False, False),
        ]
        create_grouped_opponent_table("Opponent Take-Ons", opponent_movement_metrics, team_a_stats, team_b_stats, team_a, team_b)
        
        # Opponent carries
        opponent_carries_metrics = [
            ("Progressive Carries vs Team", "opponent_progressive_carries", False, False),
            ("Carries Final Third vs Team", "opponent_carries_final_third", False, False),
            ("Carries Penalty Area vs Team", "opponent_carries_penalty_area", False, False),
        ]
        create_grouped_opponent_table("Opponent Carries", opponent_carries_metrics, team_a_stats, team_b_stats, team_a, team_b)
        
        st.markdown("**Lower values = Better at limiting opponent movement**")
        st.caption("Shows how successfully opponents move the ball when facing each team")
    
    with tab4:
        # What opponents create
        opponent_creation_metrics = [
            ("SCA vs Team", "opponent_shot_creating_actions", False, False),  # Lower is better for defending team
            ("GCA vs Team", "opponent_goal_creating_actions", False, False),
            ("SCA per 90 vs Team", "opponent_sca_per_90", False, False),
            ("GCA per 90 vs Team", "opponent_gca_per_90", False, False),
        ]
        create_grouped_opponent_table("Opponent Chance Creation", opponent_creation_metrics, team_a_stats, team_b_stats, team_a, team_b)
        
        st.markdown("**Lower values = Better at preventing opponent creativity**")
        st.caption("Shows how many chances opponents create when facing each team")

# Summary Section
st.markdown("---")
st.subheader("Analysis Summary")

summary_col1, summary_col2 = st.columns(2)

with summary_col1:
    st.write("**Direct Comparison Insights:**")
    
    # Calculate some key differences for summary
    if team_a_stats.get('goals_for', 0) > team_b_stats.get('goals_for', 0):
        st.write(f"• {team_a} scores more goals ({team_a_stats.get('goals_for', 0):.2f} vs {team_b_stats.get('goals_for', 0):.2f})")
    else:
        st.write(f"• {team_b} scores more goals ({team_b_stats.get('goals_for', 0):.2f} vs {team_a_stats.get('goals_for', 0):.2f})")
    
    if team_a_stats.get('goals_against', 0) < team_b_stats.get('goals_against', 0):
        st.write(f"• {team_a} concedes fewer goals ({team_a_stats.get('goals_against', 0):.2f} vs {team_b_stats.get('goals_against', 0):.2f})")
    else:
        st.write(f"• {team_b} concedes fewer goals ({team_b_stats.get('goals_against', 0):.2f} vs {team_a_stats.get('goals_against', 0):.2f})")
    
    pass_acc_diff = team_a_stats.get('pass_accuracy', 0) - team_b_stats.get('pass_accuracy', 0)
    if abs(pass_acc_diff) > 1:
        if pass_acc_diff > 0:
            st.write(f"• {team_a} has better pass accuracy ({team_a_stats.get('pass_accuracy', 0):.1f}% vs {team_b_stats.get('pass_accuracy', 0):.1f}%)")
        else:
            st.write(f"• {team_b} has better pass accuracy ({team_b_stats.get('pass_accuracy', 0):.1f}% vs {team_a_stats.get('pass_accuracy', 0):.1f}%)")
    
    xg_diff = team_a_stats.get('xg_for', 0) - team_b_stats.get('xg_for', 0)
    if abs(xg_diff) > 0.2:
        if xg_diff > 0:
            st.write(f"• {team_a} creates better chances (xG: {team_a_stats.get('xg_for', 0):.2f} vs {team_b_stats.get('xg_for', 0):.2f})")
        else:
            st.write(f"• {team_b} creates better chances (xG: {team_b_stats.get('xg_for', 0):.2f} vs {team_a_stats.get('xg_for', 0):.2f})")

with summary_col2:
    st.write("**Defensive Impact Insights:**")
    
    # Compare opponent performance against each team
    opp_goals_diff = team_a_stats.get('goals_against', 0) - team_b_stats.get('goals_against', 0)
    if abs(opp_goals_diff) > 0.1:
        if opp_goals_diff < 0:
            st.write(f"• Opponents score less vs {team_a} ({team_a_stats.get('goals_against', 0):.2f} vs {team_b_stats.get('goals_against', 0):.2f})")
        else:
            st.write(f"• Opponents score less vs {team_b} ({team_b_stats.get('goals_against', 0):.2f} vs {team_a_stats.get('goals_against', 0):.2f})")
    
    opp_pass_acc_diff = team_a_stats.get('opponent_pass_accuracy', 0) - team_b_stats.get('opponent_pass_accuracy', 0)
    if abs(opp_pass_acc_diff) > 1:
        if opp_pass_acc_diff < 0:
            st.write(f"• {team_a} disrupts opponent passing better (opps: {team_a_stats.get('opponent_pass_accuracy', 0):.1f}% vs {team_b_stats.get('opponent_pass_accuracy', 0):.1f}%)")
        else:
            st.write(f"• {team_b} disrupts opponent passing better (opps: {team_b_stats.get('opponent_pass_accuracy', 0):.1f}% vs {team_a_stats.get('opponent_pass_accuracy', 0):.1f}%)")
    
    # Opponent creation comparison
    opp_sca_diff = team_a_stats.get('opponent_shot_creating_actions', 0) - team_b_stats.get('opponent_shot_creating_actions', 0)
    if abs(opp_sca_diff) > 1:
        if opp_sca_diff < 0:
            st.write(f"• {team_a} limits opponent creativity better (opps create {team_a_stats.get('opponent_shot_creating_actions', 0):.2f} vs {team_b_stats.get('opponent_shot_creating_actions', 0):.2f} SCA)")
        else:
            st.write(f"• {team_b} limits opponent creativity better (opps create {team_b_stats.get('opponent_shot_creating_actions', 0):.2f} vs {team_a_stats.get('opponent_shot_creating_actions', 0):.2f} SCA)")
    
    # Opponent possession comparison
    opp_poss_diff = team_a_stats.get('opponent_possession_pct', 0) - team_b_stats.get('opponent_possession_pct', 0)
    if abs(opp_poss_diff) > 2:
        if opp_poss_diff < 0:
            st.write(f"• {team_a} limits opponent possession better (opps: {team_a_stats.get('opponent_possession_pct', 0):.1f}% vs {team_b_stats.get('opponent_possession_pct', 0):.1f}%)")
        else:
            st.write(f"• {team_b} limits opponent possession better (opps: {team_b_stats.get('opponent_possession_pct', 0):.1f}% vs {team_a_stats.get('opponent_possession_pct', 0):.1f}%)")

st.markdown("---")
st.caption(f"Data shown as {view_mode.lower()} • Left column: Direct team comparison • Right column: How opponents perform against each team")