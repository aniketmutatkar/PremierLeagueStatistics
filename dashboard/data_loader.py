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
    Get list of available players INCLUDING GOALKEEPERS (from both tables)
    """
    with PlayerAnalyzer() as analyzer:
        filter_clause, _ = analyzer._parse_timeframe(timeframe)
        
        # Build base queries for both tables
        outfield_query = f"""
            SELECT DISTINCT player_name, position, squad, minutes_played
            FROM analytics_players 
            WHERE {filter_clause} AND minutes_played >= {min_minutes}
        """
        
        keeper_query = f"""
            SELECT DISTINCT player_name, position, squad, minutes_played
            FROM analytics_keepers
            WHERE {filter_clause} AND minutes_played >= {min_minutes}
        """
        
        # Apply position filter
        if position_filter and position_filter != "All":
            if position_filter == "GK":
                # Only get keepers, exclude all outfield
                outfield_query += " AND 1=0"  
                keeper_query += " AND position LIKE '%GK%'"
            elif position_filter == "FW":
                position_list = "'FW', 'FW,MF', 'MF,FW', 'FW,DF', 'DF,FW'"
                outfield_query += f" AND position IN ({position_list})"
                keeper_query += " AND 1=0"  # Exclude keepers
            elif position_filter == "MF":
                position_list = "'MF', 'MF,FW', 'FW,MF', 'DF,MF', 'MF,DF'"
                outfield_query += f" AND position IN ({position_list})"
                keeper_query += " AND 1=0"
            elif position_filter == "DF":
                position_list = "'DF', 'DF,MF', 'MF,DF', 'DF,FW', 'FW,DF'"
                outfield_query += f" AND position IN ({position_list})"
                keeper_query += " AND 1=0"
        
        # Apply squad filter to both
        if squad_filter and squad_filter != "All":
            outfield_query += f" AND squad = '{squad_filter}'"
            keeper_query += f" AND squad = '{squad_filter}'"
        
        # UNION both queries
        combined_query = f"""
            {outfield_query}
            UNION ALL
            {keeper_query}
            ORDER BY minutes_played DESC
        """
        
        players_df = analyzer.conn.execute(combined_query).fetchdf()
        return players_df['player_name'].tolist()


@st.cache_data(ttl=3600)
def get_player_filters(timeframe="current", min_minutes=180):
    """Get available filter options INCLUDING GK from both tables"""    
    with PlayerAnalyzer() as analyzer:
        filter_clause, _ = analyzer._parse_timeframe(timeframe)
        
        # Get positions from BOTH tables
        positions_outfield = analyzer.conn.execute(f"""
            SELECT DISTINCT position
            FROM analytics_players
            WHERE {filter_clause} AND minutes_played >= {min_minutes}
        """).fetchdf()
        
        positions_keepers = analyzer.conn.execute(f"""
            SELECT DISTINCT position
            FROM analytics_keepers
            WHERE {filter_clause} AND minutes_played >= {min_minutes}
        """).fetchdf()
        
        # Combine and map to primary groups
        import pandas as pd
        all_positions = pd.concat([positions_outfield, positions_keepers])['position'].unique()
        
        position_groups = set()
        for pos in all_positions:
            if 'GK' in pos:
                position_groups.add('GK')
            elif 'FW' in pos:
                position_groups.add('FW')
            elif 'MF' in pos:
                position_groups.add('MF')
            elif 'DF' in pos:
                position_groups.add('DF')
        
        # Get squads from BOTH tables
        squads_outfield = analyzer.conn.execute(f"""
            SELECT DISTINCT squad FROM analytics_players
            WHERE {filter_clause} AND minutes_played >= {min_minutes}
        """).fetchdf()
        
        squads_keepers = analyzer.conn.execute(f"""
            SELECT DISTINCT squad FROM analytics_keepers
            WHERE {filter_clause} AND minutes_played >= {min_minutes}
        """).fetchdf()
        
        all_squads = pd.concat([squads_outfield, squads_keepers])['squad'].unique()
        
        return {
            'positions': sorted(list(position_groups)),
            'squads': sorted(list(all_squads))
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

@st.cache_data(ttl=3600)
def get_max_minutes_played(timeframe="current"):
    """
    Get maximum minutes played across all players for the given timeframe.
    Queries both analytics_players and analytics_keepers tables.
    Rounds up to nearest 90 (full match) for clean slider values.
    
    Args:
        timeframe: "current" or "season_YYYY-YYYY"
        
    Returns:
        int: Maximum minutes played, rounded up to nearest 90
    """
    with PlayerAnalyzer() as analyzer:
        # Get the same filter clause used by other queries
        filter_clause, _ = analyzer._parse_timeframe(timeframe)
        
        # Query max minutes from outfield players
        max_outfield = analyzer.conn.execute(f"""
            SELECT MAX(minutes_played) as max_minutes
            FROM analytics_players
            WHERE {filter_clause}
        """).fetchone()[0]
        
        # Query max minutes from goalkeepers
        max_keepers = analyzer.conn.execute(f"""
            SELECT MAX(minutes_played) as max_minutes
            FROM analytics_keepers
            WHERE {filter_clause}
        """).fetchone()[0]
        
        # Get the overall maximum
        max_minutes = max(
            max_outfield if max_outfield is not None else 0,
            max_keepers if max_keepers is not None else 0
        )
        
        # Handle edge case: no data
        if max_minutes == 0:
            return 90  # Default to at least one match
        
        # Round up to nearest 90 (full match)
        import math
        rounded_max = math.ceil(max_minutes / 90) * 90
        
        return rounded_max

# ============================================================================
# PLAYER OVERVIEW LOADING - ADD TO dashboard/data_loader.py
# ============================================================================

@st.cache_data(ttl=3600)
def load_player_overview(timeframe="current", position_filter=None, min_minutes=180):
    """
    Load comprehensive player overview with position percentiles for all players
    
    Args:
        timeframe: "current" or "season_YYYY-YYYY"
        position_filter: "GK", "DF", "MF", "FW", or None for all positions
        min_minutes: Minimum minutes threshold (default 180)
        
    Returns:
        pd.DataFrame with columns:
            - player_name, position, primary_position, squad, minutes_played
            - overall_score (average of 8 position percentiles)
            - 8 category position percentiles
    """
    
    with PlayerAnalyzer() as analyzer:
        # Get filtered player list
        available_players = get_available_players(timeframe, position_filter, None, min_minutes)
        
        if not available_players:
            return pd.DataFrame()
        
        player_records = []
        
        for player_name in available_players:
            # Get dual percentiles (cached)
            profile = analyzer.calculate_dual_percentiles(player_name, timeframe)
            
            if "error" in profile:
                continue
            
            player_info = profile['player_info']
            category_scores = profile['category_scores']
            
            # Get basic info (cached)
            basic_info = analyzer.get_player_basic_info(player_name, timeframe)
            
            if "error" in basic_info:
                continue
            
            # Extract position percentiles for 8 categories
            position_percentiles = []
            category_data = {}
            
            for category, data in category_scores.items():
                pos_score = data.get('position_score')
                category_data[f"{category}_pos"] = pos_score
                
                if pos_score is not None:
                    position_percentiles.append(pos_score)
            
            # Calculate overall score (average of position percentiles)
            if position_percentiles:
                overall_score = round(sum(position_percentiles) / len(position_percentiles), 1)
            else:
                overall_score = None
            
            # Build record
            record = {
                'player_name': player_name,
                'position': player_info['position'],
                'primary_position': player_info['primary_position'],
                'squad': basic_info['squad'],
                'minutes_played': basic_info['minutes_played'],
                'overall_score': overall_score,
                **category_data  # Unpack 8 category columns
            }
            
            player_records.append(record)
        
        # Convert to DataFrame
        df = pd.DataFrame(player_records)
        
        # Sort by overall_score descending
        if not df.empty and 'overall_score' in df.columns:
            df = df.sort_values('overall_score', ascending=False).reset_index(drop=True)
        
        return df

@st.cache_data(ttl=3600)
def load_player_category_leaderboard(category, timeframe="current", position_filter=None, n=10):
    """
    Get top N players for a specific category (by OVERALL percentile for league-wide comparison)
    Now supports BOTH outfield and goalkeeper categories
    
    Args:
        category: Category name (e.g., 'attacking_output' OR 'shot_stopping')
        timeframe: "current" or "season_YYYY-YYYY"
        position_filter: Filter by position group or None for all
        n: Number of top players to return (default 10)
        
    Returns:
        DataFrame with columns:
            - rank: int (1, 2, 3, ...)
            - player_name: str
            - position: str
            - squad: str
            - score: float (OVERALL percentile - for cross-position comparison)
    """
    
    with PlayerAnalyzer() as analyzer:
        # Get filtered player list
        available_players = get_available_players(timeframe, position_filter, None, min_minutes=180)
        
        if not available_players:
            return pd.DataFrame(columns=['rank', 'player_name', 'position', 'squad', 'score'])
        
        player_records = []
        
        for player_name in available_players:
            # Get dual percentiles
            profile = analyzer.calculate_dual_percentiles(player_name, timeframe)
            
            if "error" in profile:
                continue
            
            player_info = profile['player_info']
            category_scores = profile['category_scores']
            
            # Get basic info
            basic_info = analyzer.get_player_basic_info(player_name, timeframe)
            
            if "error" in basic_info:
                continue
            
            # Get OVERALL score for this category (works for both outfield and GK categories)
            if category in category_scores:
                overall_score = category_scores[category].get('overall_score')
                
                if overall_score is not None:
                    player_records.append({
                        'player_name': player_name,
                        'position': player_info['position'],
                        'squad': basic_info['squad'],
                        'score': overall_score
                    })
        
        # Convert to DataFrame
        df = pd.DataFrame(player_records)
        
        if df.empty:
            return pd.DataFrame(columns=['rank', 'player_name', 'position', 'squad', 'score'])
        
        # Sort by overall score descending
        df = df.sort_values('score', ascending=False)
        
        # Take top N
        top_n = df.head(n)
        
        # Add rank
        top_n = top_n.copy()
        top_n.insert(0, 'rank', range(1, len(top_n) + 1))
        
        return top_n[['rank', 'player_name', 'position', 'squad', 'score']]