#!/usr/bin/env python3
"""
Data Loader for Squad Analytics Dashboard
==========================================

Caching layer that wraps SquadAnalyzer for Streamlit.
Handles all database interactions and data transformations.
"""

import streamlit as st
import sys
import pandas as pd
from pathlib import Path

# Add analysis directory to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / 'analysis'))

from squad_analyzer import SquadAnalyzer # type: ignore
from player_analyzer import PlayerAnalyzer # type: ignore

# ============================================================================
# BASIC DATA QUERIES
# ============================================================================

@st.cache_data(ttl=3600)
def get_available_squads(timeframe="current"):
    """
    Get list of available squad names for given timeframe
    
    Returns:
        list: Squad names sorted alphabetically
    """
    with SquadAnalyzer() as analyzer:
        filter_clause, _ = analyzer._parse_timeframe(timeframe)
        
        query = f"""
            SELECT DISTINCT squad_name 
            FROM analytics_squads 
            WHERE {filter_clause}
            ORDER BY squad_name
        """
        
        squads_df = analyzer.conn.execute(query).fetchdf()
        return squads_df['squad_name'].tolist()


@st.cache_data(ttl=3600)
def get_available_seasons():
    """
    Get list of available seasons from database
    
    Returns:
        list: Season strings (e.g., ['2024-2025', '2023-2024']) newest first
    """
    with SquadAnalyzer() as analyzer:
        query = """
            SELECT DISTINCT season 
            FROM analytics_squads 
            ORDER BY season DESC
        """
        
        seasons_df = analyzer.conn.execute(query).fetchdf()
        return seasons_df['season'].tolist()


@st.cache_data(ttl=3600)
def get_category_names():
    """
    Get list of all 9 statistical categories
    
    Returns:
        list: Category names
    """
    with SquadAnalyzer() as analyzer:
        return list(analyzer.stat_categories.keys())


# ============================================================================
# SQUAD PROFILE LOADING
# ============================================================================

@st.cache_data(ttl=3600)
def get_squad_league_context(squad_name, timeframe="current"):
    """
    Get squad's league table context (position, points, GD, form)
    
    Returns:
        dict: {
            'position': int,
            'points': int,
            'goal_difference': int,
            'form': str (e.g., 'W-W-D-L-W')
        }
    """
    with SquadAnalyzer() as analyzer:
        # Get league table
        table = analyzer.calculate_league_table(timeframe)
        
        # Find this squad
        squad_row = table[table['squad_name'] == squad_name]
        
        if squad_row.empty:
            return {
                'position': None,
                'points': None,
                'goal_difference': None,
                'form': None
            }
        
        squad_data = squad_row.iloc[0]
        
        return {
            'position': int(squad_data['position']),
            'points': int(squad_data['points']),
            'goal_difference': int(squad_data['goal_difference']),
            'wins': int(squad_data['wins']),
            'draws': int(squad_data['draws']),
            'losses': int(squad_data['losses'])
        }

@st.cache_data(ttl=3600)
def load_squad_profile(squad_name, timeframe="current"):
    """
    Load comprehensive squad profile
    
    Args:
        squad_name: Squad name
        timeframe: "current" or "season_YYYY-YYYY"
        
    Returns:
        dict: Comprehensive profile with basic_info and dual_percentiles
    """
    with SquadAnalyzer() as analyzer:
        return analyzer.get_comprehensive_squad_profile(squad_name, timeframe)


# ============================================================================
# COMPARISON LOADING
# ============================================================================

@st.cache_data(ttl=3600)
def load_comparison(squad1, squad2, timeframe="current"):
    """
    Load head-to-head squad comparison using existing compare_squads method
    
    Args:
        squad1: First squad name
        squad2: Second squad name
        timeframe: "current" or "season_YYYY-YYYY"
        
    Returns:
        dict: Comparison with structure:
            - squads: [squad1, squad2]
            - timeframe_info: string description
            - category_comparison: {category: {scores, winner, difference}}
            - summary: {category_wins, overall_winner}
    """
    with SquadAnalyzer() as analyzer:
        return analyzer.compare_squads(squad1, squad2, timeframe)


# ============================================================================
# CATEGORY BREAKDOWN LOADING
# ============================================================================

@st.cache_data(ttl=3600)
def load_category_breakdown(squad_name, category, timeframe="current"):
    """
    Load detailed breakdown of a specific category with individual metrics
    
    Args:
        squad_name: Squad name
        category: Category name (e.g., 'attacking_output')
        timeframe: "current" or "season_YYYY-YYYY"
        
    Returns:
        dict: Category breakdown with:
            - squad_name
            - category
            - description
            - composite_score
            - rank
            - gap_from_first
            - metric_details: list of individual metrics with ranks
    """
    with SquadAnalyzer() as analyzer:
        return analyzer.get_category_breakdown(squad_name, category, timeframe)


# ============================================================================
# HELPER FUNCTIONS FOR DATA TRANSFORMATION
# ============================================================================

def extract_radar_data(squad_profile, use_composite=True):
    """
    Extract data for radar chart from squad profile
    
    Args:
        squad_profile: Output from load_squad_profile()
        use_composite: If True, use composite scores; if False, use percentiles
        
    Returns:
        tuple: (categories, values)
            - categories: list of 9 category names
            - values: list of 9 scores (0-100)
    """
    if "error" in squad_profile:
        return [], []
    
    category_scores = squad_profile['dual_percentiles']['category_scores']
    
    categories = []
    values = []
    
    for category, data in category_scores.items():
        categories.append(category)
        
        if use_composite:
            # Use composite score (0-100)
            score = data.get('composite_score', 0)
        else:
            # Use percentile (0-100)
            score = data.get('overall_score', 0)
        
        values.append(score if score is not None else 0)
    
    return categories, values


def extract_category_comparison_table(comparison_data, timeframe="current"):
    """
    Extract category comparison data for table display
    
    Args:
        comparison_data: Output from load_comparison()
        timeframe: Timeframe to use for loading profiles
        
    Returns:
        list: List of dicts with comparison data
    """
    if "error" in comparison_data:
        return []
    
    squad1, squad2 = comparison_data['squads']
    
    # Load individual profiles WITH THE CORRECT TIMEFRAME
    profile1 = load_squad_profile(squad1, timeframe)  # ← FIXED
    profile2 = load_squad_profile(squad2, timeframe)  # ← FIXED
    
    category_scores1 = profile1['dual_percentiles']['category_scores']
    category_scores2 = profile2['dual_percentiles']['category_scores']
    
    table_data = []
    
    for category, comp in comparison_data['category_comparison'].items():
        rank1 = category_scores1[category].get('rank')
        rank2 = category_scores2[category].get('rank')
        comp1 = category_scores1[category].get('composite_score')
        comp2 = category_scores2[category].get('composite_score')
        
        # Determine winner based on RANK (lower rank = better)
        if rank1 is not None and rank2 is not None:
            if rank1 < rank2:
                winner = squad1
            elif rank2 < rank1:
                winner = squad2
            else:
                winner = "tie"
        else:
            winner = "tie"
        
        table_data.append({
            'category': category,
            'squad1_composite': comp1,
            'squad1_rank': rank1,
            'squad2_composite': comp2,
            'squad2_rank': rank2,
            'winner': winner
        })
    
    return table_data


def extract_metric_breakdown(breakdown_data):
    """
    Extract individual metrics for drill-down display
    
    Args:
        breakdown_data: Output from load_category_breakdown()
        
    Returns:
        list: List of dicts with metric data
    """
    if "error" in breakdown_data:
        return []
    
    metrics = []
    
    for metric_detail in breakdown_data.get('metric_details', []):
        # Convert rank to int if it's a string
        rank = metric_detail.get('rank')
        if rank is not None and isinstance(rank, str):
            try:
                rank = int(rank)
            except:
                rank = None
        
        metrics.append({
            'metric': metric_detail['metric'],
            'value': metric_detail.get('value'),
            'rank': rank,
            'total_squads': metric_detail.get('total_squads', 20)
        })
    
    return metrics


@st.cache_data(ttl=3600)
def load_league_overview(timeframe="current"):
    """
    Load complete league overview with traditional stats + composite scores
    
    Args:
        timeframe: "current" or "season_YYYY-YYYY"
        
    Returns:
        DataFrame with columns:
            - position, squad_name, points, goal_difference, wins, draws, losses
            - overall_composite (NEW - average of all 9 categories)
            - attacking_output, creativity, passing, ball_progression, defending, 
              physical_duels, possession, team_performance (9 composites)
    """
    with SquadAnalyzer() as analyzer:
        # Get league table with traditional stats
        league_table = analyzer.calculate_league_table(timeframe)
    
    # Build complete dataset with composites
    overview_data = []
    
    for _, row in league_table.iterrows():
        squad_name = row['squad_name']
        
        # Get squad profile (cached, so fast)
        profile = load_squad_profile(squad_name, timeframe)
        
        # Skip if error
        if "error" in profile:
            continue
        
        # Extract composite scores from profile
        category_scores = profile['dual_percentiles']['category_scores']
        
        # Build row
        squad_row = {
            'position': int(row['position']),
            'squad_name': squad_name,
            'points': int(row['points']),
            'goal_difference': int(row['goal_difference']),
            'wins': int(row['wins']),
            'draws': int(row['draws']),
            'losses': int(row['losses'])
        }
        
        # Add composite scores (9 categories) and collect for overall calculation
        composite_values = []
        for category, data in category_scores.items():
            composite_score = data.get('composite_score', None)
            squad_row[category] = composite_score
            if composite_score is not None:
                composite_values.append(composite_score)
        
        # Calculate overall composite (average of all categories)
        if composite_values:
            squad_row['overall_composite'] = sum(composite_values) / len(composite_values)
        else:
            squad_row['overall_composite'] = None
        
        overview_data.append(squad_row)
    
    # Convert to DataFrame
    df = pd.DataFrame(overview_data)
    
    return df

@st.cache_data(ttl=3600)
def load_category_leaderboard(category, timeframe="current", n=5):
    """
    Get top N squads for a specific category
    
    Args:
        category: Category name (e.g., 'attacking_output')
        timeframe: "current" or "season_YYYY-YYYY"
        n: Number of top squads to return (default 5)
        
    Returns:
        DataFrame with columns:
            - rank: int (1, 2, 3, ...)
            - squad_name: str
            - composite_score: float (0-100)
    """
    with SquadAnalyzer() as analyzer:
        # Get all composite scores for this category
        composite_results = analyzer.calculate_category_composite_scores(category, timeframe)
    
    # Check if we got results
    if composite_results.empty:
        return pd.DataFrame(columns=['rank', 'squad_name', 'composite_score'])
    
    # Take top N
    top_n = composite_results.head(n)
    
    # Return only the columns we need
    return top_n[['rank', 'squad_name', 'composite_score']].copy()

# ============================================================================
# PLAYER DATA QUERIES
# ============================================================================
# Add these functions to dashboard/data_loader.py after the squad functions

@st.cache_data(ttl=3600)
def get_available_players(timeframe="current", position_filter=None, squad_filter=None, min_minutes=180):
    """
    Get list of available players with optional filters
    
    Args:
        timeframe: "current" or "season_YYYY-YYYY"
        position_filter: Filter by position group (e.g., "FW", "MF", "DF", "GK") or None for all
        squad_filter: Filter by squad name or None for all
        min_minutes: Minimum minutes played (default 180 = 2 games)
        
    Returns:
        list: Player names sorted by minutes played (descending)
    """    

    with PlayerAnalyzer() as analyzer:
        filter_clause, _ = analyzer._parse_timeframe(timeframe)
        
        # Build query with filters
        query = f"""
            SELECT DISTINCT player_name, position, squad, minutes_played
            FROM analytics_players 
            WHERE {filter_clause} AND minutes_played >= {min_minutes}
        """
        
        # Add position filter if specified
        if position_filter and position_filter != "All":
            # Use position groups to handle hybrid positions
            if position_filter == "FW":
                position_list = "'FW', 'FW,MF', 'MF,FW', 'FW,DF', 'DF,FW'"
            elif position_filter == "MF":
                position_list = "'MF', 'MF,FW', 'FW,MF', 'DF,MF', 'MF,DF'"
            elif position_filter == "DF":
                position_list = "'DF', 'DF,MF', 'MF,DF', 'DF,FW', 'FW,DF'"
            elif position_filter == "GK":
                position_list = "'GK'"
            else:
                position_list = f"'{position_filter}'"
            
            query += f" AND position IN ({position_list})"
        
        # Add squad filter if specified
        if squad_filter and squad_filter != "All":
            query += f" AND squad = '{squad_filter}'"
        
        query += " ORDER BY minutes_played DESC"
        
        players_df = analyzer.conn.execute(query).fetchdf()
        return players_df['player_name'].tolist()


@st.cache_data(ttl=3600)
def get_player_filters(timeframe="current", min_minutes=180):
    """
    Get available filter options for players
    
    Args:
        timeframe: "current" or "season_YYYY-YYYY"
        min_minutes: Minimum minutes played
        
    Returns:
        dict: {
            'positions': list of unique positions,
            'squads': list of unique squads
        }
    """
    
    with PlayerAnalyzer() as analyzer:
        filter_clause, _ = analyzer._parse_timeframe(timeframe)
        
        # Get unique positions
        positions_query = f"""
            SELECT DISTINCT position
            FROM analytics_players
            WHERE {filter_clause} AND minutes_played >= {min_minutes}
            ORDER BY position
        """
        positions_df = analyzer.conn.execute(positions_query).fetchdf()
        
        # Map positions to primary groups
        position_groups = set()
        for pos in positions_df['position'].tolist():
            if 'GK' in pos:
                position_groups.add('GK')
            elif 'FW' in pos:
                position_groups.add('FW')
            elif 'MF' in pos:
                position_groups.add('MF')
            elif 'DF' in pos:
                position_groups.add('DF')
        
        # Get unique squads
        squads_query = f"""
            SELECT DISTINCT squad
            FROM analytics_players
            WHERE {filter_clause} AND minutes_played >= {min_minutes}
            ORDER BY squad
        """
        squads_df = analyzer.conn.execute(squads_query).fetchdf()
        
        return {
            'positions': sorted(list(position_groups)),
            'squads': squads_df['squad'].tolist()
        }


# ============================================================================
# PLAYER PROFILE LOADING
# ============================================================================

@st.cache_data(ttl=3600)
def load_player_profile(player_name, timeframe="current"):
    """
    Load comprehensive player profile with dual percentiles
    
    Args:
        player_name: Player name
        timeframe: "current" or "season_YYYY-YYYY"
        
    Returns:
        dict: Comprehensive profile with:
            - basic_info: player name, position, squad, minutes
            - dual_percentiles: category_scores with overall and position percentiles
            - player_insights: strengths, weaknesses, versatility
    """
    
    with PlayerAnalyzer() as analyzer:
        return analyzer.get_comprehensive_player_profile(player_name, timeframe)


# ============================================================================
# PLAYER COMPARISON LOADING
# ============================================================================

@st.cache_data(ttl=3600)
def load_player_comparison(player1, player2, timeframe="current"):
    """
    Load head-to-head player comparison
    
    Args:
        player1: First player name
        player2: Second player name
        timeframe: "current" or "season_YYYY-YYYY"
        
    Returns:
        dict: Comparison with structure:
            - players: [player1, player2]
            - timeframe_info: description
            - category_comparison: {category: {scores, winner, difference}}
            - summary: {category_wins, overall_winner}
    """
    
    with PlayerAnalyzer() as analyzer:
        return analyzer.compare_players_comprehensive(player1, player2, timeframe)


# ============================================================================
# PLAYER CATEGORY BREAKDOWN LOADING
# ============================================================================

@st.cache_data(ttl=3600)
def load_player_category_breakdown(player_name, category, timeframe="current"):
    """
    Load detailed breakdown of a specific category for a player
    
    Args:
        player_name: Player name
        category: Category name (e.g., 'attacking_output')
        timeframe: "current" or "season_YYYY-YYYY"
        
    Returns:
        dict: Category breakdown with:
            - player_name
            - category
            - description
            - composite_score
            - metric_details: list of metrics with overall and position percentiles
    """
    
    with PlayerAnalyzer() as analyzer:
        return analyzer.get_category_breakdown(player_name, category, timeframe)


# ============================================================================
# SIMILAR PLAYERS LOADING
# ============================================================================

@st.cache_data(ttl=3600)
def load_similar_players(player_name, timeframe="current", top_n=10, same_position_only=True):
    """
    Find statistically similar players
    
    Args:
        player_name: Target player name
        timeframe: "current" or "season_YYYY-YYYY"
        top_n: Number of similar players to return (default 10)
        same_position_only: Limit to same position group (default True)
        
    Returns:
        dict: Similar players with:
            - target_player: {name, position, category_profile}
            - similar_players: list of {player_name, position, similarity_score}
            - comparison_method: string
            - categories_compared: list
    """
    
    
    with PlayerAnalyzer() as analyzer:
        return analyzer.find_similar_players(player_name, timeframe, top_n, same_position_only)


# ============================================================================
# HELPER FUNCTIONS FOR PLAYER DATA TRANSFORMATION
# ============================================================================

def extract_player_radar_data(player_profile):
    """
    Extract dual percentile data for radar chart from player profile
    
    Args:
        player_profile: Output from load_player_profile()
        
    Returns:
        tuple: (categories, overall_scores, position_scores)
            - categories: list of category names
            - overall_scores: list of overall percentiles (0-100)
            - position_scores: list of position percentiles (0-100)
    """
    if "error" in player_profile:
        return [], [], []
    
    category_scores = player_profile['dual_percentiles']['category_scores']
    
    categories = []
    overall_scores = []
    position_scores = []
    
    for category, data in category_scores.items():
        categories.append(category)
        overall_scores.append(data['overall_score'] if data['overall_score'] is not None else 0)
        position_scores.append(data['position_score'] if data['position_score'] is not None else 0)
    
    return categories, overall_scores, position_scores


def extract_player_category_table_data(player_profile):
    """
    Extract category breakdown for table display
    
    Args:
        player_profile: Output from load_player_profile()
        
    Returns:
        list: List of dicts with category data for table
    """
    if "error" in player_profile:
        return []
    
    category_scores = player_profile['dual_percentiles']['category_scores']
    
    table_data = []
    for category, data in category_scores.items():
        table_data.append({
            'category': category,
            'description': data['description'],
            'overall_score': data['overall_score'],
            'position_score': data['position_score'],
            'metrics_analyzed': data['metrics_analyzed']
        })
    
    return table_data


def extract_player_basic_info(player_profile):
    """Extract basic player info for display"""
    if "error" in player_profile:
        return {}
    
    basic = player_profile.get('basic_info', {})
    
    return {
        'player_name': basic.get('player_name', 'N/A'),
        'position': basic.get('position', 'N/A'),
        'primary_position': basic.get('primary_position', 'N/A'),
        'squad': basic.get('squad', 'N/A'),
        'age': basic.get('age', 'N/A'),
        'minutes_played': basic.get('minutes_played', 0),
        'matches_played': basic.get('matches_played', 0)
    }