#!/usr/bin/env python3
"""
Data Loader for Squad Analytics Dashboard
==========================================

Caching layer that wraps SquadAnalyzer for Streamlit.
Handles all database interactions and data transformations.
"""

import streamlit as st
import sys
from pathlib import Path

# Add analysis directory to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / 'analysis'))

from squad_analyzer import SquadAnalyzer # type: ignore


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