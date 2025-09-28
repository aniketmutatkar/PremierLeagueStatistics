import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import duckdb

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Page configuration
st.set_page_config(page_title="Premier League Overview", layout="wide", initial_sidebar_state="collapsed")

# Database connection
@st.cache_resource
def get_database_connection():
    """Create database connection"""
    return duckdb.connect("data/premierleague_raw.duckdb")

# Real data queries
@st.cache_data
def load_real_data():
    """Load all real data from database"""
    conn = get_database_connection()
    
    # Get current gameweek
    current_gw_query = """
    SELECT MAX(current_through_gameweek) as current_gw 
    FROM raw_fixtures
    """
    current_gw = conn.execute(current_gw_query).fetchone()[0]
    
    # Calculate league table from fixtures
    league_table_query = """
    WITH team_stats AS (
        -- Home team stats
        SELECT 
            home_team as team,
            COUNT(*) as matches_played,
            SUM(home_score) as goals_for,
            SUM(away_score) as goals_against,
            SUM(CASE 
                WHEN home_score > away_score THEN 3
                WHEN home_score = away_score THEN 1
                ELSE 0
            END) as points
        FROM raw_fixtures 
        WHERE is_completed = true
        GROUP BY home_team
        
        UNION ALL
        
        -- Away team stats  
        SELECT 
            away_team as team,
            COUNT(*) as matches_played,
            SUM(away_score) as goals_for,
            SUM(home_score) as goals_against,
            SUM(CASE 
                WHEN away_score > home_score THEN 3
                WHEN away_score = home_score THEN 1
                ELSE 0
            END) as points
        FROM raw_fixtures 
        WHERE is_completed = true
        GROUP BY away_team
    ),
    aggregated_stats AS (
        SELECT 
            team,
            SUM(matches_played) as mp,
            SUM(goals_for) as gf,
            SUM(goals_against) as ga,
            SUM(goals_for) - SUM(goals_against) as gd,
            SUM(points) as pts
        FROM team_stats
        GROUP BY team
    )
    SELECT 
        ROW_NUMBER() OVER (ORDER BY pts DESC, gd DESC, gf DESC) as pos,
        team,
        pts,
        mp,
        gf,
        ga,
        gd
    FROM aggregated_stats
    ORDER BY pts DESC, gd DESC, gf DESC
    """
    
    table_data = conn.execute(league_table_query).fetchdf()
    table_data.columns = ["Pos", "Team", "Pts", "MP", "GF", "GA", "GD"]
    
    # Top scorers
    top_scorers_query = """
    SELECT Player, Squad, "Performance Gls" as goals
    FROM player_standard 
    WHERE "Performance Gls" > 0
    ORDER BY "Performance Gls" DESC 
    LIMIT 3
    """
    top_scorers_df = conn.execute(top_scorers_query).fetchdf()
    top_scorers = [(row['Player'], row['Squad'], row['goals']) for _, row in top_scorers_df.iterrows()]
    
    # Top assists
    top_assists_query = """
    SELECT Player, Squad, "Performance Ast" as assists
    FROM player_standard 
    WHERE "Performance Ast" > 0
    ORDER BY "Performance Ast" DESC 
    LIMIT 3
    """
    top_assists_df = conn.execute(top_assists_query).fetchdf()
    top_assists = [(row['Player'], row['Squad'], row['assists']) for _, row in top_assists_df.iterrows()]
    
    # Team form - last 5 matches per team
    team_form_query = """
    WITH all_matches AS (
        SELECT 
            home_team as team,
            gameweek,
            match_date,
            CASE 
                WHEN home_score > away_score THEN 'W'
                WHEN home_score = away_score THEN 'D'
                ELSE 'L'
            END as result
        FROM raw_fixtures 
        WHERE is_completed = true
        
        UNION ALL
        
        SELECT 
            away_team as team,
            gameweek,
            match_date,
            CASE 
                WHEN away_score > home_score THEN 'W'
                WHEN away_score = home_score THEN 'D'
                ELSE 'L'
            END as result
        FROM raw_fixtures 
        WHERE is_completed = true
    ),
    ranked_matches AS (
        SELECT 
            team,
            result,
            ROW_NUMBER() OVER (PARTITION BY team ORDER BY gameweek DESC, match_date DESC) as rn
        FROM all_matches
    )
    SELECT team, result
    FROM ranked_matches 
    WHERE rn <= 5
    ORDER BY team, rn
    """
    
    form_df = conn.execute(team_form_query).fetchdf()
    
    # Convert to team_form dictionary
    team_form = {}
    for team in form_df['team'].unique():
        team_matches = form_df[form_df['team'] == team]['result'].tolist()
        # Pad with draws if less than 5 matches
        while len(team_matches) < 5:
            team_matches.append('D')
        team_form[team] = team_matches[:5]
    
    # Get top 5 teams by points for form display
    top_teams = table_data.head(5)['Team'].tolist()
    team_form = {team: team_form.get(team, ['D']*5) for team in top_teams}
    
    # Recent results (previous gameweek)
    recent_results_query = f"""
    SELECT home_team, away_team, home_score, away_score
    FROM raw_fixtures 
    WHERE gameweek = {current_gw - 1} AND is_completed = true
    ORDER BY match_date
    """
    recent_df = conn.execute(recent_results_query).fetchdf()
    recent_results = [f"{row['home_team']} {int(row['home_score'])}-{int(row['away_score'])} {row['away_team']}" 
                 for _, row in recent_df.iterrows()]
    
    # Upcoming fixtures (current gameweek)
    upcoming_fixtures_query = f"""
    SELECT match_date, match_time, home_team, away_team
    FROM raw_fixtures 
    WHERE gameweek = {current_gw} AND is_completed = false
    ORDER BY match_date, match_time
    """
    upcoming_df = conn.execute(upcoming_fixtures_query).fetchdf()
    
    upcoming_fixtures = []
    for _, row in upcoming_df.iterrows():
        date_str = row['match_date'].strftime('%b %d') if pd.notna(row['match_date']) else 'TBD'
        time_str = str(row['match_time'])[:5] if pd.notna(row['match_time']) else 'TBD'
        upcoming_fixtures.append((f"{date_str} {time_str}", f"{row['home_team']} v {row['away_team']}"))
    
    return table_data, top_scorers, top_assists, team_form, recent_results, upcoming_fixtures, current_gw

def style_dataframe_for_positions(df):
    """Apply conditional formatting for league positions"""
    def highlight_positions(row):
        if row['Pos'] <= 4:
            return ['background-color: rgba(0, 210, 106, 0.1)'] * len(row)
        elif row['Pos'] >= 18:
            return ['background-color: rgba(255, 75, 75, 0.1)'] * len(row)
        else:
            return [''] * len(row)
    
    return df.style.apply(highlight_positions, axis=1)

# Load real data
table_data, top_scorers, top_assists, team_form, recent_results, upcoming_fixtures, current_gw = load_real_data()

# Header
st.title("⚽ Premier League Overview")
col1, col2 = st.columns([7, 1])
with col2:
    st.text(f"Current Gameweek {current_gw}")

st.divider()

# Main 3-column layout
col1, col2, col3 = st.columns([2, 1, 1.2])

# Column 1: League Table
with col1:
    st.subheader("🏆 League Table")
    
    # Create styled dataframe
    styled_df = style_dataframe_for_positions(table_data)
    
    st.dataframe(
        table_data,
        hide_index=True,
        height=750,
        use_container_width=True,
        column_config={
            "Pos": st.column_config.NumberColumn("Pos"),
            "Team": st.column_config.TextColumn("Team"),
            "Pts": st.column_config.NumberColumn("Pts"),
            "MP": st.column_config.NumberColumn("MP"),
            "GF": st.column_config.NumberColumn("GF"),
            "GA": st.column_config.NumberColumn("GA"),
            "GD": st.column_config.NumberColumn("GD")
        }
    )

# Column 2: League Leaders
with col2:
    st.subheader("📊 League Leaders")
    
    # Top Scorers
    st.markdown("⚽ **Top Scorers**")
    with st.container(height=180):
        for i, (player, team, goals) in enumerate(top_scorers, 1):
            st.write(f"{i}. {player} ({goals})")
        
    # Top Assists
    st.markdown("🎯 **Top Assists**")
    with st.container(height=180): 
        for i, (player, team, assists) in enumerate(top_assists, 1):
            st.write(f"{i}. {player} ({assists})")
        
     # Team Form
    st.markdown("📈 **Team Form (Last 5)**")
    with st.container(height=230):
        for team, form_list in team_form.items():
            emoji_form = ""
            for result in form_list:
                if result == "W":
                    emoji_form += "🟢"
                elif result == "L": 
                    emoji_form += "🔴"
                else:  # Draw
                    emoji_form += "⚪"
            
            col_team, col_form = st.columns([3, 2])
            with col_team:
                st.write(team)
            with col_form:
                st.write(emoji_form)

# Column 3: Fixtures & Results
with col3:
    st.subheader("⚽ Fixtures & Form")
    
    # Results container
    st.markdown(f"**GW{current_gw-1} Results**")
    with st.container(height=325):
        for result in recent_results:
            st.write(f"{result}")
    
    # Fixtures container
    st.markdown(f"**GW{current_gw} Fixtures**")
    with st.container(height=325):
        for date_time, match in upcoming_fixtures:
            st.write(f"{date_time} - {match}")

st.divider()

# Bottom section: Goal Statistics
st.subheader("📈 Team Goal Statistics")

chart_col1, chart_col2 = st.columns(2)

# Goals Scored Chart
with chart_col1:
    st.markdown("**Goals Scored**")
    goals_scored_df = table_data[['Team', 'GF']].sort_values('GF', ascending=False)
    
    fig_scored = px.bar(
        goals_scored_df, 
        x='GF', 
        y='Team',
        orientation='h',
        template='plotly_dark',
        color='GF',
        color_continuous_scale='Greens',
        text='GF'
    )
    fig_scored.update_traces(textposition='outside')
    fig_scored.update_layout(
        height=600,
        showlegend=False,
        yaxis={'categoryorder': 'total ascending'},
        xaxis_title="Goals Scored",
        yaxis_title="",
        coloraxis_showscale=False
    )
    st.plotly_chart(fig_scored, use_container_width=True)

# Goals Conceded Chart
with chart_col2:
    st.markdown("**Goals Conceded**")
    goals_conceded_df = table_data[['Team', 'GA']].sort_values('GA', ascending=True)
    
    fig_conceded = px.bar(
        goals_conceded_df,
        x='GA',
        y='Team', 
        orientation='h',
        template='plotly_dark',
        color='GA',
        color_continuous_scale='Reds_r',  # Reverse red scale (lighter = better)
        text='GA'
    )
    fig_conceded.update_traces(textposition='outside')
    fig_conceded.update_layout(
        height=600,
        showlegend=False,
        yaxis={'categoryorder': 'total descending'},
        xaxis_title="Goals Conceded",
        yaxis_title="",
        coloraxis_showscale=False
    )
    st.plotly_chart(fig_conceded, use_container_width=True)