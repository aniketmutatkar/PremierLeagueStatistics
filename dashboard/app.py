#!/usr/bin/env python3
"""
Squad Analytics Dashboard
=========================

Clean, modern analytics dashboard for Premier League squad comparison.
"""

import streamlit as st
import sys
import pandas as pd
from pathlib import Path

# Add project paths
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / 'dashboard'))
sys.path.insert(0, str(project_root / 'analysis'))

from player_analyzer import PlayerAnalyzer #type: ignore

from data_loader import (
    get_available_squads,
    get_available_seasons,
    get_category_names,
    load_squad_profile,
    load_comparison,
    load_category_breakdown,
    extract_radar_data,
    extract_category_comparison_table,
    extract_metric_breakdown,
    get_squad_league_context,
    load_league_overview,      
    load_category_leaderboard,  
    get_available_players,   
    get_player_filters,      
    load_player_profile,     
    load_player_comparison,  
    load_player_category_breakdown,
    load_similar_players,    
    extract_player_radar_data,
    extract_player_category_table_data,
    extract_player_basic_info,
    load_player_overview,
    load_player_category_leaderboard,
    get_max_gameweeks_played,
    load_squad_roster,
    load_squad_profile_with_context
)

from charts import (
    create_radar_chart,
    create_category_table,
    create_metric_drilldown_table,
    create_league_table,       
    create_category_heatmap,   
    create_category_leaderboard,
    create_category_winners_chart,
    create_position_vs_quality_scatter,       
    create_player_dual_radar_chart,
    create_player_category_table,  
    create_player_comparison_radar,
    create_player_comparison_table,
    create_similar_players_table,  
    create_player_rankings_table,
    create_player_category_heatmap,
    create_goalkeeper_heatmap,
    create_player_category_leaderboard_table,
    create_squad_dominance_charts,
    create_squad_roster_table,
    create_squad_category_context_table
)

# ============================================================================
# HELPER FUNCTION
# ============================================================================

def _get_ordinal_suffix(n):
    """Get ordinal suffix for a number (1st, 2nd, 3rd, 4th, etc.)"""
    if 10 <= n % 100 <= 20:
        suffix = 'th'
    else:
        suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')
    return suffix

def is_negative_metric(metric_name):
    """Check if a metric is negative (lower is better)"""
    # Import PlayerAnalyzer to access NEGATIVE_METRICS
    with PlayerAnalyzer() as analyzer:
        return metric_name in analyzer.NEGATIVE_METRICS

@st.cache_data(ttl=3600)
def get_player_position(player_name, timeframe="current"):
    """Get player position quickly without loading full profile"""
    with PlayerAnalyzer() as analyzer:
        filter_clause, _ = analyzer._parse_timeframe(timeframe)
        
        result = analyzer.conn.execute(f"""
            SELECT position
            FROM analytics_players
            WHERE player_name = ? AND {filter_clause}
            LIMIT 1
        """, [player_name]).fetchone()
        
        return result[0] if result else ""

# ============================================================================
# PAGE CONFIG
# ============================================================================

st.set_page_config(
    page_title="Squad Analytics",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    /* Main title styling */
    .main-title {
        font-size: 2.5rem;
        font-weight: 700;
        margin-bottom: 0.5rem;
    }
    
    /* Metric cards */
    .metric-card {
        background-color: rgba(255, 255, 255, 0.05);
        padding: 1.5rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    
    /* Winner badge */
    .winner-badge {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1rem 2rem;
        border-radius: 0.5rem;
        text-align: center;
        font-size: 1.5rem;
        font-weight: 600;
    }
    
    /* Insight box */
    .insight-box {
        background-color: rgba(46, 204, 113, 0.1);
        border-left: 4px solid #2ecc71;
        padding: 1rem;
        margin: 0.5rem 0;
        border-radius: 0.25rem;
    }
    
    /* Section headers */
    .section-header {
        font-size: 1.75rem;
        font-weight: 600;
        margin-top: 2rem;
        margin-bottom: 1rem;
        border-bottom: 2px solid rgba(255, 255, 255, 0.1);
        padding-bottom: 0.5rem;
    }
    
    /* Hide default streamlit elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* Adjust padding */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
</style>
""", unsafe_allow_html=True)


# ============================================================================
# SIDEBAR
# ============================================================================

with st.sidebar:
    # ========================================================================
    # HEADER
    # ========================================================================
    st.title("⚽ Squad Analytics")
    st.caption("Premier League statistical analysis and squad comparison tool")
    
    st.markdown("---")
    
    # ========================================================================
    # PAGE NAVIGATION
    # ========================================================================
    page = st.radio(
        "Navigate",
        ["League Overview", "Squad Comparison", "Squad Profiles", "Player Overview", "Player Analysis"],
        index=0,
        label_visibility="collapsed"
    )
    
    st.markdown("---")
    
    # ========================================================================
    # SEASON SELECTOR (ALWAYS VISIBLE)
    # ========================================================================
    st.subheader("⚙️ Settings")
    
    seasons = get_available_seasons()
    
    if seasons:
        selected_season = st.selectbox(
            "Season",
            seasons,
            index=0,
            label_visibility="visible"
        )
        
        # Convert to timeframe format
        if selected_season == seasons[0]:
            timeframe = "current"
        else:
            timeframe = f"season_{selected_season}"
    else:
        st.error("No seasons available")
        st.stop()

    # ========================================================================
    # SQUAD SELECTOR (ONLY FOR SQUAD PROFILES PAGE)
    # ========================================================================
    if page == "Squad Profiles":
        st.markdown("---")
        st.subheader("🏟️ Squad Selector")
        
        squads = get_available_squads(timeframe)
        
        if squads:
            selected_squad = st.selectbox(
                "Select Squad",
                squads,
                index=0,
                key="squad_profile_selector"
            )
        else:
            st.error("No squads available")
            st.stop()
    
    # ========================================================================
    # SQUAD SELECTORS (ONLY FOR SQUAD COMPARISON PAGE)
    # ========================================================================
    if page == "Squad Comparison":
        st.markdown("---")
        
        squads = get_available_squads(timeframe)
        
        if not squads or len(squads) < 2:
            st.error("Not enough squads available")
            st.stop()
        
        squad1 = st.selectbox("Squad 1", squads, index=0)
        squad2 = st.selectbox("Squad 2", squads, index=1 if len(squads) > 1 else 0)
    
    # ========================================================================
    # PLAYER SELECTORS (ONLY FOR PLAYER ANALYSIS PAGE)
    # ========================================================================
    if page == "Player Analysis":
        st.markdown("---")
        
        # Initialize session state for player selection if not exists
        if 'selected_player' not in st.session_state:
            st.session_state.selected_player = None
        
        # Get filter options
        filter_options = get_player_filters(timeframe, min_minutes=90)
        
        # Advanced filters (collapsible)
        with st.expander("🔍 Filters", expanded=False):
            position_filter = st.selectbox(
                "Position",
                ["All"] + filter_options['positions'],
                index=0
            )
            
            squad_filter = st.selectbox(
                "Squad",
                ["All"] + filter_options['squads'],
                index=0
            )
            
            # Get dynamic max gameweeks based on actual data
            max_gameweeks_available = get_max_gameweeks_played(timeframe)

            min_gameweeks = st.slider(
                "Minimum Gameweeks Played",
                min_value=1,
                max_value=max_gameweeks_available,
                value=max(2, round(max_gameweeks_available / 2)),
                step=1,
                help="Filter players who have played at least this many full matches (1 gameweek = 90 minutes)"
            )

            # Convert gameweeks to minutes for backend filtering
            min_minutes = min_gameweeks * 90
            
            same_position_only = st.checkbox(
                "Same position only (similar players)",
                value=True,
                help="Limit similar players to same position group"
            )
            
            min_similarity = st.slider(
                "Similarity Threshold",
                min_value=50,
                max_value=95,
                value=75,
                step=5,
                help="Minimum similarity % for similar players section"
            )
        
        # Get filtered player list
        try:
            pos_filter = position_filter if position_filter != "All" else None
        except:
            pos_filter = None
        
        try:
            sq_filter = squad_filter if squad_filter != "All" else None
        except:
            sq_filter = None
        
        try:
            min_mins = min_minutes
        except:
            min_mins = 180
        
        try:
            same_pos = same_position_only
        except:
            same_pos = True
        
        try:
            min_sim = min_similarity
        except:
            min_sim = 75
        
        available_players = get_available_players(
            timeframe,
            pos_filter,
            sq_filter,
            min_mins
        )
        
        if not available_players:
            st.error("No players found with selected filters")
            st.stop()
        
        # Main player selector with session state persistence
        # Find index of previously selected player if it exists in current list
        default_index = 0
        if st.session_state.selected_player and st.session_state.selected_player in available_players:
            default_index = available_players.index(st.session_state.selected_player)
        
        selected_player = st.selectbox(
            "Select Player",
            available_players,
            index=default_index,
            key="player_selector"  # Give it a unique key
        )
        
        # Update session state when selection changes
        if selected_player != st.session_state.selected_player:
            st.session_state.selected_player = selected_player


    st.markdown("---")
    st.caption("💾 Data refreshes after each gameweek")
    st.caption("📊 8 statistical categories analyzed")

# ============================================================================
# LEAGUE OVERVIEW PAGE
# ============================================================================

def show_league_overview():
    """League Overview page with table, scatter plot, heatmap, winners chart, and top 5 rankings"""
    
    st.markdown('<div class="main-title">Premier League Overview</div>', unsafe_allow_html=True)
    st.caption(f"Season: {selected_season}")
    st.markdown("---")
    
    # Load data
    with st.spinner("Loading league data..."):
        df = load_league_overview(timeframe)
    
    if df.empty:
        st.error("No data available for this season")
        return
    
    # ========================================================================
    # SECTION 1: LEAGUE TABLE + SCATTER PLOT (SIDE BY SIDE)
    # ========================================================================
    col_left, col_right = st.columns([0.45, 0.55])
    
    with col_left:
        st.subheader("League Table")
        st.dataframe(
            create_league_table(df),
            hide_index=True,
            use_container_width=True,
            height=750
        )
    
    with col_right:
        st.subheader("League Position vs Quality")
    
        # Add interpretation caption ABOVE the graph
        st.caption("""
        Teams above the line are **underperforming** (better quality than league position suggests).  
        Teams below the line are **overperforming** (worse quality than league position suggests).  
        """)
        
        # Create scatter plot and get outlier data
        scatter_fig, overperformers, underperformers = create_position_vs_quality_scatter(df)
        st.plotly_chart(scatter_fig, use_container_width=True)
        
        # Add subtle footnote-style summary
        overperformers_sorted = sorted(overperformers, key=lambda x: x[2], reverse=True)
        underperformers_sorted = sorted(underperformers, key=lambda x: x[2])
        
        # Build compact text strings
        under_text = ", ".join([f"{squad} ({pos}{_get_ordinal_suffix(pos)}, +{residual:.1f})" 
                                for squad, pos, residual in overperformers_sorted])
        over_text = ", ".join([f"{squad} ({pos}{_get_ordinal_suffix(pos)}, {residual:.1f})" 
                            for squad, pos, residual in underperformers_sorted])
        
        st.caption(f"**Biggest Underperformers:** {under_text} | **Biggest Overperformers:** {over_text}")
    
    # ========================================================================
    # SECTION 2: CATEGORY PERFORMANCE HEATMAP (UPGRADED)
    # ========================================================================
    
    st.markdown('<div class="section-header">Category Performance Heatmap</div>', unsafe_allow_html=True)
    st.markdown("Visual representation of each squad's performance across all categories. Numbers show rank (1-20), colors show composite score.")
    
    heatmap_fig = create_category_heatmap(df)
    st.plotly_chart(heatmap_fig, use_container_width=True)
    
    # ========================================================================
    # SECTION 3: CATEGORY WINNERS BAR CHART (NEW)
    # ========================================================================
    
    st.markdown('<div class="section-header">Category Leaders</div>', unsafe_allow_html=True)
    st.markdown("Which squad leads in each statistical category?")
    
    winners_fig = create_category_winners_chart(df)
    st.plotly_chart(winners_fig, use_container_width=True)
    
    # ========================================================================
    # SECTION 4: TOP 5 RANKINGS (EXPANDABLE - EXISTING)
    # ========================================================================
    
    st.markdown('<div class="section-header">Top 5 by Category</div>', unsafe_allow_html=True)
    st.markdown("Detailed rankings for each statistical category.")
    
    categories = get_category_names()
    
    for category in categories:
        # Clean category name for display
        category_display = category.replace('_', ' ').title()
        
        with st.expander(f"🏆 {category_display}"):
            leaderboard_df = load_category_leaderboard(category, timeframe, n=5)
            
            if leaderboard_df.empty:
                st.info("No data available for this category")
            else:
                st.dataframe(
                    create_category_leaderboard(leaderboard_df, category),
                    hide_index=True,
                    use_container_width=True
                )

# ============================================================================
# PLAYER ANALYSIS PAGE
# ============================================================================
# Add this function to dashboard/app.py after show_league_overview()


def show_player_analysis(timeframe, selected_player, min_similarity, same_position_only):
    """
    Player Analysis page - all sections use the selected_player from sidebar
    
    Args:
        timeframe: Season timeframe
        selected_player: Player selected in sidebar
        min_similarity: Minimum similarity threshold from sidebar
        same_position_only: Whether to limit similar players to same position
    """
    
    # ========================================================================
    # HEADER & TITLE
    # ========================================================================
    
    st.markdown(f'<div class="main-title">Premier League Player Analysis</div>', unsafe_allow_html=True)

    # Load player profile
    player_profile = load_player_profile(selected_player, timeframe)

    # Extract data
    player_info = extract_player_basic_info(player_profile)
    categories, overall_scores, position_scores = extract_player_radar_data(player_profile)
    category_table_data = extract_player_category_table_data(player_profile)

    if overall_scores:
        overall_avg = sum(overall_scores) / len(overall_scores)
    elif position_scores:
        overall_avg = sum(position_scores) / len(position_scores)
    else:
        overall_avg = 0.0

    st.markdown(f"#### {selected_player}   |   Overall Score: {overall_avg:.1f}", unsafe_allow_html=True)
    st.caption(f"**Season**: {timeframe} | **Position:** {player_info.get('position', 'N/A')} | **Squad**: {player_info.get('squad', 'N/A')} | **Minutes**: {player_info.get('minutes_played', 0)}")
    
    if "error" in player_profile:
        st.error(f"Error loading player profile: {player_profile['error']}")
        return
    
    context_col1, context_col2 = st.columns(2)

    # ========================================================================
    # SECTION 1: PERFORMANCE OVERVIEW
    # ========================================================================
    
    st.markdown('<div class="section-header">Performance Overview</div>', unsafe_allow_html=True)
    st.caption("Percentile rankings across 8 categories (dual percentile: overall league vs position group)")
    
    # Two columns: Radar chart and Category table
    col_left, col_right = st.columns([1, 1])
    
    with col_left:
        st.subheader("Radar Chart")
        st.caption("Visual performance profile")
        
        radar_fig = create_player_dual_radar_chart(
            selected_player,
            categories,
            overall_scores,
            position_scores
        )
        if radar_fig:
            st.plotly_chart(radar_fig, use_container_width=True)
        st.caption("Position percentile vs position group (50% = average)")
    
    with col_right:
        st.subheader("Category Breakdown")
        st.caption("Overall and position-specific percentiles")
        
        category_table = create_player_category_table(category_table_data)
        if category_table is not None:
            st.dataframe(
                category_table,
                use_container_width=True,
                hide_index=True
            )
        else:
            st.warning("No category data available")
    
    # ========================================================================
    # DETAILED METRICS - EXPANDABLE DRILL-DOWN
    # ========================================================================
    
    st.markdown('<div class="section-header">Detailed Metric Breakdown</div>', unsafe_allow_html=True)
    st.caption("Expand any category to see individual metrics with dual percentiles")
    
    category_scores = player_profile['dual_percentiles']['category_scores']
    
    for category_name, category_data in category_scores.items():
        category_display = category_name.replace('_', ' ').title()
        
        with st.expander(f"📊 {category_display}", expanded=False):
            st.caption(category_data.get('description', ''))
            
            # Load detailed breakdown
            breakdown = load_player_category_breakdown(selected_player, category_name, timeframe)
            
            if "error" not in breakdown:
                metrics_data = breakdown.get('metric_details', [])
                
                if metrics_data:
                    # Create metrics table
                    table_rows = []
                    for metric in metrics_data:
                        metric_name = metric.get('metric', 'Unknown')
                        value = metric.get('value')
                        overall_pct = metric.get('overall_percentile')
                        position_pct = metric.get('position_percentile')
                        
                        # Format value
                        if value is not None:
                            if isinstance(value, float):
                                value_str = f"{value:.2f}" if value < 100 else f"{value:.0f}"
                            else:
                                value_str = str(value)
                        else:
                            value_str = "—"
                        
                        # Format percentiles
                        overall_str = f"{overall_pct:.1f}%" if overall_pct is not None else "—"
                        position_str = f"{position_pct:.1f}%" if position_pct is not None else "—"
                        
                        table_rows.append({
                            'Metric': metric_name.replace('_', ' ').title(),
                            'Value': value_str,
                            'Overall': overall_str,
                            'Position': position_str
                        })
                    
                    import pandas as pd
                    metrics_df = pd.DataFrame(table_rows)
                    
                    st.dataframe(
                        metrics_df,
                        use_container_width=True,
                        hide_index=True,
                        height=400
                    )
                else:
                    st.info("No metrics available for this category")
    
    # ========================================================================
    # SECTION 2: PLAYER COMPARISON
    # ========================================================================
    
    st.markdown("---")
    st.markdown('<div class="section-header">Player Comparison</div>', unsafe_allow_html=True)
    st.caption("Compare against another player using position percentiles")
    
    # Get available players for comparison
    if player_info.get('position') and 'GK' in player_info.get('position', ''):
        # If selected player is GK, only show other GKs
        available_players = get_available_players(timeframe, position_filter="GK", squad_filter=None, min_minutes=180)
    else:
        # If outfield player, show all outfield players (exclude GKs)
        available_players = get_available_players(timeframe, position_filter=None, squad_filter=None, min_minutes=180)
        # Filter out any GKs that might have slipped through
        available_players = [p for p in available_players if p != selected_player]

    # Remove the selected player from comparison options
    available_players = [p for p in available_players if p != selected_player]

    if len(available_players) == 0:
        st.warning("No other players available for comparison with the current filters.")
    else:
        player2 = st.selectbox(
            "Compare against:",
            available_players,
            index=0
        )
    
    # Load comparison data (player1 = selected_player, player2 = dropdown selection)
    comparison = load_player_comparison(selected_player, player2, timeframe)
    
    if "error" in comparison:
        st.error(f"Error loading comparison: {comparison['error']}")
    else:
        # Load individual profiles
        profile1 = player_profile  # Already loaded above
        profile2 = load_player_profile(player2, timeframe)
        
        # Extract data
        info1 = player_info  # Already extracted above
        info2 = extract_player_basic_info(profile2)
        
        cats1, overall1, position1 = extract_player_radar_data(profile1)
        cats2, overall2, position2 = extract_player_radar_data(profile2)
        
        # Player context side by side
        st.markdown(f"#### {selected_player} vs {player2}")
        
        context_col1, context_col2 = st.columns(2)
        
        with context_col1:
            st.markdown(f"**{selected_player}**")
            st.caption(f"{info1.get('position', 'N/A')} • {info1.get('squad', 'N/A')} • {info1.get('minutes_played', 0):,} min")
        
        with context_col2:
            st.markdown(f"**{player2}**")
            st.caption(f"{info2.get('position', 'N/A')} • {info2.get('squad', 'N/A')} • {info2.get('minutes_played', 0):,} min")
        
        # Winner badge
        summary = comparison.get('summary', {})
        overall_winner = summary.get('overall_winner', 'balanced')
        category_wins = summary.get('category_wins', {})
        
        if overall_winner != 'balanced':
            st.markdown(f"""
            <div class="winner-badge">
                🏆 Winner: {overall_winner} ({category_wins.get(selected_player, 0)}-{category_wins.get(player2, 0)} categories)
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Key insights
        category_comparison = comparison.get('category_comparison', {})

        differences = []
        for category, data in category_comparison.items():
            diff = abs(data.get('difference', 0))
            winner = data.get('winner', 'tie')
            if winner != 'tie' and diff > 0:
                differences.append({
                    'category': category.replace('_', ' ').title(),
                    'difference': diff,
                    'winner': winner
                })
        
        differences.sort(key=lambda x: x['difference'], reverse=True)
        
        if differences:
            st.markdown("**Key Insights:**")

            # Find top 3 gaps
            diff_top3 = differences[:3]
            
            col1, col2, col3 = st.columns(3)
            
            for idx, diff in enumerate(diff_top3):
                with [col1, col2, col3][idx]:
                    st.markdown(f"""
                    <div class="insight-box">
                        • <strong>{diff['category']}</strong>: {diff['winner']} leads by {diff['difference']:.1f} points
                    </div>
                    """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Two columns: Radar and comparison table
        comp_viz_col1, comp_viz_col2 = st.columns([1, 1])
        
        with comp_viz_col1:
            st.subheader("Radar Chart")
            st.caption("Position percentiles (50% baseline = average)")
            
            comp_radar = create_player_comparison_radar(
                selected_player, cats1, overall1,
                player2, cats2, overall2
            )
            if comp_radar:
                st.plotly_chart(comp_radar, use_container_width=True)
        
        with comp_viz_col2:
            st.subheader("Category Breakdown")
            st.caption("Position percentiles and winners")
            
            comp_table = create_player_comparison_table(selected_player, player2, category_comparison)
            if comp_table is not None:
                st.dataframe(
                    comp_table,
                    use_container_width=True,
                    hide_index=True
                )
        
        # Detailed metrics comparison drill-down
        st.markdown('<div class="section-header">Detailed Metric Breakdown</div>', unsafe_allow_html=True)
        st.caption("Expand any category to see individual metric comparisons")
        
        for category_name, data in category_comparison.items():
            category_display = category_name.replace('_', ' ').title()
            
            with st.expander(f"📊 {category_display}", expanded=False):
                st.caption(data.get('category_description', ''))
                
                # Load breakdowns for both players
                breakdown1 = load_player_category_breakdown(selected_player, category_name, timeframe)
                breakdown2 = load_player_category_breakdown(player2, category_name, timeframe)
                
                if "error" not in breakdown1 and "error" not in breakdown2:
                    metrics1 = {m['metric']: m for m in breakdown1.get('metric_details', [])}
                    metrics2 = {m['metric']: m for m in breakdown2.get('metric_details', [])}
                    
                    # Get union of all metrics
                    all_metrics = sorted(set(metrics1.keys()) | set(metrics2.keys()))
                    
                    # Build comparison table
                    comp_rows = []
                    for metric_name in all_metrics:
                        m1 = metrics1.get(metric_name, {})
                        m2 = metrics2.get(metric_name, {})
                        
                        val1 = m1.get('value')
                        val2 = m2.get('value')
                        
                        # Format values (2 decimals)
                        if val1 is not None:
                            val1_str = f"{val1:.2f}" if isinstance(val1, float) and val1 < 100 else str(int(val1)) if val1 is not None else "—"
                        else:
                            val1_str = "—"

                        if val2 is not None:
                            val2_str = f"{val2:.2f}" if isinstance(val2, float) and val2 < 100 else str(int(val2)) if val2 is not None else "—"
                        else:
                            val2_str = "—"

                        # Determine winner (RESPECTS negative metrics)
                        if val1 is not None and val2 is not None:
                            if is_negative_metric(metric_name):
                                # For negative metrics: LOWER is BETTER
                                if val1 < val2:
                                    winner_str = selected_player
                                elif val2 < val1:
                                    winner_str = player2
                                else:
                                    winner_str = "Tie"
                            else:
                                # For positive metrics: HIGHER is BETTER
                                if val1 > val2:
                                    winner_str = selected_player
                                elif val2 > val1:
                                    winner_str = player2
                                else:
                                    winner_str = "Tie"
                        else:
                            winner_str = "—"
                        
                        comp_rows.append({
                            'Metric': metric_name.replace('_', ' ').title(),
                            selected_player: val1_str,
                            player2: val2_str,
                            'Winner': winner_str
                        })
                    
                    import pandas as pd
                    comp_df = pd.DataFrame(comp_rows)
                    
                    # Apply styling
                    def highlight_metric_winner(row):
                        if row['Winner'] == selected_player:
                            return ['background-color: rgba(65, 105, 225, 0.1)'] * len(row)
                        elif row['Winner'] == player2:
                            return ['background-color: rgba(220, 20, 60, 0.1)'] * len(row)
                        elif row['Winner'] == "Tie":
                            return ['background-color: rgba(128, 128, 128, 0.05)'] * len(row)
                        else:
                            return [''] * len(row)
                    
                    styled_comp = comp_df.style.apply(highlight_metric_winner, axis=1)
                    
                    st.dataframe(
                        styled_comp,
                        use_container_width=True,
                        hide_index=True,
                        height=400
                    )
    
    # ========================================================================
    # SECTION 3: SIMILAR PLAYERS
    # ========================================================================
    
    st.markdown("---")
    st.markdown('<div class="section-header">Similar Players</div>', unsafe_allow_html=True)
    st.caption(f"Players with similar statistical profiles to {selected_player}")
    
    # Load similar players (using selected_player, same_position_only and min_similarity from sidebar)
    similar_data = load_similar_players(selected_player, timeframe, top_n=10, same_position_only=same_position_only)
    
    if "error" in similar_data:
        st.error(f"Error finding similar players: {similar_data['error']}")
    else:
        # Filter by minimum similarity (from sidebar)
        similar_players = similar_data.get('similar_players', [])
        filtered_similar = [p for p in similar_players if p['similarity_score'] >= min_similarity]
        
        if not filtered_similar:
            st.warning(f"⚠️ No players found with >= {min_similarity}% similarity. Adjust the threshold in the sidebar filters.")
        else:
            st.success(f"✅ Found {len(filtered_similar)} similar players")
            
            # Display similar players table
            similar_table_data = {
                'target_player': similar_data['target_player'],
                'similar_players': filtered_similar,
                'comparison_method': similar_data['comparison_method'],
                'categories_compared': similar_data['categories_compared']
            }
            
            similar_table = create_similar_players_table(similar_table_data)
            if similar_table is not None:
                st.dataframe(
                    similar_table,
                    use_container_width=True,
                    hide_index=True
                )
            
            st.caption(f"Comparison: {similar_data['comparison_method']} | Categories: {len(similar_data['categories_compared'])}")


# ============================================================================
# PLAYER OVERVIEW PAGE - ADD TO dashboard/app.py
# ============================================================================

def show_player_overview(timeframe):
    """
    Player Overview page showing all players with rankings, distribution, heatmap, and leaderboards
    Similar to League Overview but for players
    
    Args:
        timeframe: Season timeframe
    """
    
    st.markdown('<div class="main-title">Premier League Player Overview</div>', unsafe_allow_html=True)
    st.caption(f"Season: {selected_season}")
    st.markdown("---")
    
    # ========================================================================
    # FILTERS (HORIZONTAL ROW)
    # ========================================================================
    filter_col1, filter_col2 = st.columns([1, 1])
    
    with filter_col1:
        position_filter = st.selectbox(
            "Position",
            ["All", "GK", "DF", "MF", "FW"],
            index=0,
            help="Filter players by primary position group"
        )
    
    with filter_col2:
        # Get dynamic max gameweeks based on actual data
        max_gameweeks_available = get_max_gameweeks_played(timeframe)
        
        min_gameweeks = st.slider(
            "Minimum Gameweeks Played",
            min_value=1,
            max_value=max_gameweeks_available,
            value=max(2, round(max_gameweeks_available / 2)),
            step=1,
            help="Filter players who have played at least this many full matches (1 gameweek = 90 minutes)"
        )
        
    # Convert gameweeks to minutes for backend filtering
    min_minutes = min_gameweeks * 90
    
    # Convert position filter
    pos_filter = None if position_filter == "All" else position_filter
    
    # Load data
    with st.spinner("Loading player data..."):
        df = load_player_overview(timeframe, pos_filter, min_minutes)
    
    if df.empty:
        st.error("No players found with selected filters")
        return
    
    st.caption(f"✅ Loaded {len(df)} players")
    
    # ========================================================================
    # SECTION 1: PLAYER RANKINGS & DISTRIBUTION (SIDE BY SIDE)
    # ========================================================================
    st.markdown('<div class="section-header">Player Rankings & Score Distribution</div>', unsafe_allow_html=True)
    
    col_left, col_right = st.columns([0.55, 0.45])
    
    with col_left:
        st.subheader("Player Rankings")
        st.caption("All players ranked by overall score (avg of position percentiles)")
        
        # Create rankings table
        rankings_table = create_player_rankings_table(df)
        
        # Display with scrollable container
        st.dataframe(
            rankings_table,
            hide_index=True,
            use_container_width=True,
            height=450
        )
        
        st.caption("💡 Click a player row to navigate to their full profile")
    
    with col_right:
        st.subheader("Squad Dominance")
        
        # Create combined squad dominance chart
        dominance_chart = create_squad_dominance_charts(df, top_n_players=100)
        st.plotly_chart(dominance_chart, use_container_width=True)
    
    # ========================================================================
    # SECTION 2: CATEGORY PERFORMANCE HEATMAP (BY POSITION)
    # ========================================================================
    st.markdown("---")
    st.markdown('<div class="section-header">Category Performance Heatmap</div>', unsafe_allow_html=True)
    st.markdown("Top 10 players per position across all categories. Numbers show position-specific rank, colors show position percentile.")
    
    # Sortable dropdown (applies to all position tabs)
    sort_col1, sort_col2 = st.columns([1, 3])
    
    with sort_col1:
        category_options = [
            ('Overall Score', None),
            ('Attacking Output', 'attacking_output'),
            ('Creativity', 'creativity'),
            ('Passing', 'passing'),
            ('Ball Progression', 'ball_progression'),
            ('Defending', 'defending'),
            ('Physical Duels', 'physical_duels'),
            ('Ball Involvement', 'ball_involvement'),
            ('Discipline', 'discipline_reliability')
        ]
        
        sort_choice = st.selectbox(
            "Sort heatmaps by:",
            options=[opt[0] for opt in category_options],
            index=0,
            help="Re-sort players by selected category (applies to all position tabs)"
        )
        
        sort_category = next((opt[1] for opt in category_options if opt[0] == sort_choice), None)
    
    # Create tabs - REORDERED: FW, MF, DF, GK
    tab_fw, tab_mf, tab_df, tab_gk = st.tabs(["⚡ Forwards", "⚙️ Midfielders", "🛡️ Defenders", "🧤 Goalkeepers"])
    
    with tab_fw:
        if sort_category:
            st.subheader(f"Top 10 Forwards (Sorted by {sort_choice})")
        else:
            st.subheader("Top 10 Forwards")
        heatmap_fw = create_player_category_heatmap(df, sort_category, 'FW')
        st.plotly_chart(heatmap_fw, use_container_width=True)
        st.caption("Position percentiles • #1 = Highest rank")
    
    with tab_mf:
        if sort_category:
            st.subheader(f"Top 10 Midfielders (Sorted by {sort_choice})")
        else:
            st.subheader("Top 10 Midfielders")
        heatmap_mf = create_player_category_heatmap(df, sort_category, 'MF')
        st.plotly_chart(heatmap_mf, use_container_width=True)
        st.caption("Position percentiles • #1 = Highest rank")
    
    with tab_df:
        if sort_category:
            st.subheader(f"Top 10 Defenders (Sorted by {sort_choice})")
        else:
            st.subheader("Top 10 Defenders")
        heatmap_df_pos = create_player_category_heatmap(df, sort_category, 'DF')
        st.plotly_chart(heatmap_df_pos, use_container_width=True)
        st.caption("Position percentiles • #1 = Highest rank")
    
    with tab_gk:
        if sort_category:
            st.subheader(f"Top 10 Goalkeepers (Sorted by {sort_choice})")
        else:
            st.subheader("Top 10 Goalkeepers")
        
        # For goalkeepers, create a DIFFERENT heatmap with GK categories
        gk_heatmap = create_goalkeeper_heatmap(df, sort_category)
        st.plotly_chart(gk_heatmap, use_container_width=True)
        st.caption("Goalkeeper categories • #1 = Highest rank")
    
    # ========================================================================
    # SECTION 3: TOP 10 BY CATEGORY (EXPANDABLE)
    # ========================================================================
    st.markdown("---")
    st.markdown('<div class="section-header">Top 10 by Category</div>', unsafe_allow_html=True)
    st.markdown("League-wide leaders for each category (overall percentiles across all positions).")
    
    # Get all categories
    categories = [
        ('attacking_output', 'Attacking Output'),
        ('creativity', 'Creativity'),
        ('passing', 'Passing'),
        ('ball_progression', 'Ball Progression'),
        ('defending', 'Defending'),
        ('physical_duels', 'Physical Duels'),
        ('ball_involvement', 'Ball Involvement'),
        ('discipline_reliability', 'Discipline & Reliability')
    ]
    
    for category_key, category_display in categories:
        with st.expander(f"📊 {category_display}", expanded=False):
            # Load leaderboard
            leaderboard_df = load_player_category_leaderboard(
                category_key, 
                timeframe, 
                pos_filter, 
                n=10
            )
            
            if leaderboard_df.empty:
                st.info("No data available for this category")
            else:
                # Create styled table
                styled_table = create_player_category_leaderboard_table(leaderboard_df, category_display)
                
                st.dataframe(
                    styled_table,
                    hide_index=True,
                    use_container_width=True
                )
                
                st.caption(f"Top 10 players by {category_display.lower()} (overall percentile - league-wide comparison)")

    # ========================================================================
    # SECTION 4: GOALKEEPER ANALYSIS (EXPANDABLE)
    # ========================================================================
    st.markdown("---")
    st.markdown('<div class="section-header">🧤 Goalkeeper Analysis</div>', unsafe_allow_html=True)
    st.markdown("Goalkeeper-specific performance metrics and rankings")
    
    # Get goalkeepers only
    gk_df = df[df['primary_position'] == 'GK'].copy()
    
    if not gk_df.empty:
        st.caption(f"📊 {len(gk_df)} goalkeepers found")
        
        # Goalkeeper categories from PlayerAnalyzer
        gk_categories = [
            ('shot_stopping', 'Shot Stopping', 'Saves, save percentage, PSxG performance'),
            ('distribution', 'Distribution', 'Passing accuracy and distance from goal'),
            ('sweeping', 'Sweeping', 'Actions outside penalty area'),
            ('penalty_saving', 'Penalty Saving', 'Penalty kick performance'),
            ('cross_claiming', 'Cross Claiming', 'Dealing with crosses and aerial balls'),
            ('clean_sheets', 'Clean Sheets', 'Clean sheets and match results'),
            ('goals_prevented', 'Goals Prevented', 'Goals prevented above expectation'),
            ('ball_playing', 'Ball Playing', 'Progressive passing from keeper')
        ]
        
        for category_key, category_display, category_desc in gk_categories:
            with st.expander(f"🥅 {category_display}", expanded=False):
                st.caption(category_desc)
                
                # Load top 10 goalkeepers for this category
                # Note: This uses overall percentiles since all are GK
                gk_leaderboard = load_player_category_leaderboard(
                    category_key,
                    timeframe,
                    position_filter='GK',
                    n=10
                )
                
                if not gk_leaderboard.empty:
                    styled_table = create_player_category_leaderboard_table(gk_leaderboard, category_display)
                    st.dataframe(
                        styled_table,
                        hide_index=True,
                        use_container_width=True
                    )
                    st.caption(f"Top 10 goalkeepers by {category_display.lower()}")
                else:
                    st.info("No goalkeeper data available for this category")
    else:
        st.info("No goalkeepers found with selected filters")

# ============================================================================
# SQUAD PROFILES PAGE
# ============================================================================

def show_squad_profile(squad_name, timeframe):
    """
    Squad Profiles page showing:
    - Section 1: Squad Performance Overview with league context
    - Section 2: Player Roster (all players sorted by minutes)
    - Section 3: Squad Insights (top performers, strengths, weaknesses)
    
    Args:
        squad_name: Selected squad name
        timeframe: Season timeframe
    """
    
    st.markdown('<div class="main-title">Squad Profile</div>', unsafe_allow_html=True)
    
    # Load squad profile with league context
    with st.spinner(f"Loading {squad_name} profile..."):
        profile = load_squad_profile_with_context(squad_name, timeframe)
    
    if "error" in profile:
        st.error(f"Error loading squad profile: {profile['error']}")
        return
    
    # Extract data
    basic_info = profile['basic_info']
    dual_percentiles = profile['dual_percentiles']
    league_context = profile.get('league_context', {})
    squad_insights = profile['squad_insights']
    
    # Get league table context
    league_table_context = get_squad_league_context(squad_name, timeframe)
    
    # ========================================================================
    # HEADER WITH LEAGUE CONTEXT
    # ========================================================================

    # Calculate squad overall composite first (needed for header)
    category_scores = dual_percentiles['category_scores']
    squad_composite_scores = [data.get('composite_score') for data in category_scores.values() if data.get('composite_score') is not None]

    squad_overall_composite = None
    this_rank = None
    total_squads = None

    if squad_composite_scores:
        squad_overall_composite = sum(squad_composite_scores) / len(squad_composite_scores)
        
        # Get ALL squads' overall composites for ranking
        all_squads = get_available_squads(timeframe)
        
        squad_composites = []
        for other_squad in all_squads:
            other_profile = load_squad_profile(other_squad, timeframe)
            
            if "error" not in other_profile:
                other_categories = other_profile['dual_percentiles']['category_scores']
                other_scores = [data.get('composite_score') for data in other_categories.values() if data.get('composite_score') is not None]
                
                if other_scores:
                    other_overall = sum(other_scores) / len(other_scores)
                    squad_composites.append({
                        'squad_name': other_squad,
                        'overall_composite': other_overall
                    })
        
        # Sort by overall composite
        squad_composites_df = pd.DataFrame(squad_composites).sort_values('overall_composite', ascending=False).reset_index(drop=True)
        squad_composites_df['rank'] = range(1, len(squad_composites_df) + 1)
        
        # Find this squad's position
        this_squad_row = squad_composites_df[squad_composites_df['squad_name'] == squad_name]
        
        if not this_squad_row.empty:
            this_rank = int(this_squad_row.iloc[0]['rank'])
            total_squads = len(squad_composites_df)

    # Build header with squad quality integrated
    header_text = f"## {squad_name}"

    if squad_overall_composite is not None and this_rank is not None:
        suffix = _get_ordinal_suffix(this_rank)
        header_text += f" • {squad_overall_composite:.1f} ({this_rank}{suffix} of {total_squads})"

    st.markdown(header_text)

    # League table context (Season, Position, Points, GD)
    if league_table_context.get('position'):
        position = league_table_context['position']
        points = league_table_context['points']
        gd = league_table_context['goal_difference']
        
        suffix = _get_ordinal_suffix(position)
        gd_str = f"+{gd}" if gd > 0 else str(gd)
        
        st.caption(f"**Season:** {basic_info['season']} | **League Position:** {position}{suffix} | **Points:** {points} | **Goal Difference:** {gd_str}")

    # Nearby teams (compact, under caption)
    if squad_composite_scores and not this_squad_row.empty:
        # Get nearby squads (±2 positions)
        start_idx = max(0, this_rank - 3)
        end_idx = min(total_squads, this_rank + 2)
        nearby_squads = squad_composites_df.iloc[start_idx:end_idx]
        
        # Build compact nearby teams string
        nearby_text = ""
        nearby_parts = []
        
        for _, row in nearby_squads.iterrows():
            other_squad = row['squad_name']
            other_rank = int(row['rank'])
            other_score = row['overall_composite']
            
            if other_squad == squad_name:
                nearby_parts.append(f"\n{other_rank}. **{other_squad}** ({other_score:.1f})")
            else:
                diff = other_score - squad_overall_composite
                diff_str = f"+{diff:.1f}" if diff > 0 else f"{diff:.1f}"
                nearby_parts.append(f"\n{other_rank}. {other_squad} ({other_score:.1f}, {diff_str})")
        
        nearby_text += "".join(nearby_parts)
        st.caption(nearby_text)

    st.markdown("---")

    # ========================================================================
    # SECTION 1: SQUAD PERFORMANCE OVERVIEW
    # ========================================================================
    
    st.markdown('<div class="section-header">Squad Performance Overview</div>', unsafe_allow_html=True)
    st.caption("Squad performance across 8 categories with league rankings")
    
    col_left, col_right = st.columns([1, 1])
    
    with col_left:
        st.subheader("Performance Radar")
        st.caption("Composite scores (0-100 scale)")
        
        # Extract radar data - single squad
        categories, values = extract_radar_data(profile, use_composite=True)
        
        if categories and values:
            # Create single-squad radar (reuse existing function with squad2 as None)
            radar_fig = create_radar_chart(
                squad_name, categories, values,
                None, [], []  # No second squad
            )
            st.plotly_chart(radar_fig, use_container_width=True)
    
    with col_right:
        st.subheader("Category Rankings")
        st.caption("Composite scores and league position")
        
        # Create category context table
        context_table = create_squad_category_context_table(profile)
        
        if context_table is not None:
            st.dataframe(
                context_table,
                use_container_width=True,
                hide_index=True,
                height=400
            )
        else:
            st.warning("No category data available")
    
    # ========================================================================
    # SECTION 2: PLAYER ROSTER
    # ========================================================================
    
    st.markdown('<div class="section-header">Player Roster</div>', unsafe_allow_html=True)
    st.caption("All players with 1+ minutes played, sorted by minutes")
    
    with st.spinner("Loading player roster..."):
        roster_df = load_squad_roster(squad_name, timeframe)
    
    if roster_df.empty:
        st.warning(f"No players found for {squad_name}")
    else:
        st.caption(f"✅ {len(roster_df)} players in squad")
        
        roster_table = create_squad_roster_table(roster_df)
        
        if roster_table is not None:
            st.dataframe(
                roster_table,
                use_container_width=True,
                hide_index=True,
                height=600
            )
        else:
            st.error("Error formatting roster table")
    
    # ========================================================================
    # SECTION 3: SQUAD INSIGHTS
    # ========================================================================

    st.markdown('<div class="section-header">Squad Insights</div>', unsafe_allow_html=True)

    # ========================================================================
    # TOP PERFORMERS BY POSITION (3 COLUMNS)
    # ========================================================================

    st.subheader("🌟 Top Performers by Position")
    st.caption("Top 3 players in each position group (ranked by position percentile average)")

    if not roster_df.empty:
        # Separate players by position group
        forwards = roster_df[roster_df['position'].str.contains('FW', na=False)]
        midfielders = roster_df[roster_df['position'].str.contains('MF', na=False) & ~roster_df['position'].str.contains('FW', na=False)]
        defenders = roster_df[roster_df['position'].str.contains('DF', na=False) & ~roster_df['position'].str.contains('MF', na=False)]
        goalkeepers = roster_df[roster_df['position'].str.contains('GK', na=False)]
        
        # Sort each group by position_avg (descending)
        forwards = forwards.sort_values('position_avg', ascending=False, na_position='last')
        midfielders = midfielders.sort_values('position_avg', ascending=False, na_position='last')
        defenders = defenders.sort_values('position_avg', ascending=False, na_position='last')
        goalkeepers = goalkeepers.sort_values('position_avg', ascending=False, na_position='last')
        
        # Create columns based on whether GK exists
        if not goalkeepers.empty:
            col_fw, col_mf, col_df = st.columns(3)
        else:
            col_fw, col_mf, col_df = st.columns(3)
        
        # FORWARDS
        with col_fw:
            st.markdown("#### **Forwards**")
            if not forwards.empty:
                top_fw = forwards.head(3)
                for idx, (_, player) in enumerate(top_fw.iterrows(), start=1):
                    name = player['player_name']
                    pos_avg = player['position_avg']
                    top_cat = player['top_category']
                    top_pos = player['top_category_position']
                    indent = "&nbsp;" * 3
                    st.markdown(f"{indent}{idx}. **{name}**")
                    st.caption(f"{indent}{pos_avg:.1f}% (Elite in {top_cat}: {top_pos:.1f}%)")
            else:
                st.info("No forwards")
        
        # MIDFIELDERS
        with col_mf:
            st.markdown("#### **Midfielders**")
            if not midfielders.empty:
                top_mf = midfielders.head(3)
                for idx, (_, player) in enumerate(top_mf.iterrows(), start=1):
                    name = player['player_name']
                    pos_avg = player['position_avg']
                    top_cat = player['top_category']
                    top_pos = player['top_category_position']
                    indent = "&nbsp;" * 3
                    st.markdown(f"{indent}{idx}. **{name}**")
                    st.caption(f"{indent}{pos_avg:.1f}% (Elite in {top_cat}: {top_pos:.1f}%)")
            else:
                st.info("No midfielders")
        
        # DEFENDERS
        with col_df:
            st.markdown("#### **Defenders**")
            if not defenders.empty:
                top_df = defenders.head(3)
                for idx, (_, player) in enumerate(top_df.iterrows(), start=1):
                    name = player['player_name']
                    pos_avg = player['position_avg']
                    top_cat = player['top_category']
                    top_pos = player['top_category_position']
                    indent = "&nbsp;" * 3
                    st.markdown(f"{indent}{idx}. **{name}**")
                    st.caption(f"{indent}{pos_avg:.1f}% (Elite in {top_cat}: {top_pos:.1f}%)")
            else:
                st.info("No defenders")

    else:
        st.info("No player data available")

    st.markdown("---")

    # ========================================================================
    # SQUAD STRENGTHS AND WEAKNESSES (EXISTING - KEEP THIS)
    # ========================================================================

    col_strengths, col_weaknesses = st.columns(2)

    with col_strengths:
        st.subheader("💪 Squad Strengths")
        st.caption("Categories with 60%+ composite score")
        
        strengths = squad_insights.get('top_strengths', [])
        
        if strengths:
            for strength in strengths:
                category = strength['category'].replace('_', ' ').title()
                score = strength['score']
                rank = strength['rank']
                
                # Get league context
                context = league_context.get(strength['category'], {})
                total = context.get('total_squads', 20)
                
                st.markdown(f"• **{category}**: {score:.1f}% (Rank: {rank}/{total})")
        else:
            st.info("No standout strengths identified (no category above 60%)")

    with col_weaknesses:
        st.subheader("⚠️ Squad Weaknesses")
        st.caption("Categories with 40%- composite score")
        
        weaknesses = squad_insights.get('main_weaknesses', [])
        
        if weaknesses:
            for weakness in weaknesses:
                category = weakness['category'].replace('_', ' ').title()
                score = weakness['score']
                rank = weakness['rank']
                
                # Get league context
                context = league_context.get(weakness['category'], {})
                total = context.get('total_squads', 20)
                
                st.markdown(f"• **{category}**: {score:.1f}% (Rank: {rank}/{total})")
        else:
            st.info("No critical weaknesses identified (no category below 40%)")

# ============================================================================
# SQUAD COMPARISON PAGE
# ============================================================================

if page == "Squad Comparison":
    # Check if same squad selected
    if squad1 == squad2:
        st.warning("⚠️ Please select two different squads to compare")
        st.stop()

    # Load data
    try:
        comparison = load_comparison(squad1, squad2, timeframe)
        profile1 = load_squad_profile(squad1, timeframe)
        profile2 = load_squad_profile(squad2, timeframe)
        
        # Check for errors
        if "error" in comparison:
            st.error(f"Comparison error: {comparison['error']}")
            st.stop()
        
        if "error" in profile1:
            st.error(f"{squad1} error: {profile1['error']}")
            st.stop()
        
        if "error" in profile2:
            st.error(f"{squad2} error: {profile2['error']}")
            st.stop()

    except Exception as e:
        st.error(f"Failed to load data: {e}")
        import traceback
        st.code(traceback.format_exc())
        st.stop()


    # ============================================================================
    # HEADER & TITLE
    # ============================================================================

    st.markdown(f'<div class="main-title">Premier League Squad Comparison</div>', unsafe_allow_html=True)
    st.markdown(f'#### {squad1} vs {squad2}', unsafe_allow_html=True)
    st.caption(f"Season: {selected_season}")
    st.markdown("---")


    # ============================================================================
    # SQUAD CONTEXT - League Position & Stats
    # ============================================================================

    # Get league context for both squads
    context1 = get_squad_league_context(squad1, timeframe)
    context2 = get_squad_league_context(squad2, timeframe)

    # Determine winner for visual highlighting
    summary = comparison.get('summary', {})
    category_wins = summary.get('category_wins', {})
    overall_winner = summary.get('overall_winner', 'tie')

    wins1 = category_wins.get(squad1, 0)
    wins2 = category_wins.get(squad2, 0)

    # Display in two columns with tables
    col1, col2 = st.columns(2)

    with col1:
        # Add subtle highlighting if this squad is winning
        if overall_winner == squad1:
            st.markdown(f"### {squad1} 🏆")
        else:
            st.markdown(f"### {squad1}")
        
        # Create table data
        table_data = {
            'Metric': ['Position', 'Points', 'Goal Difference', 'Record'],
            'Value': [
                f"{context1['position']}" if context1['position'] else "N/A",
                f"{context1['points']}" if context1['points'] is not None else "N/A",
                f"+{context1['goal_difference']}" if context1['goal_difference'] is not None and context1['goal_difference'] >= 0 else str(context1['goal_difference']) if context1['goal_difference'] is not None else "N/A",
                f"{context1['wins']}W - {context1['draws']}D - {context1['losses']}L" if context1['wins'] is not None else "N/A"
            ]
        }
        
        df1 = pd.DataFrame(table_data)
        st.dataframe(df1, use_container_width=True, hide_index=True)

    with col2:
        # Add subtle highlighting if this squad is winning
        if overall_winner == squad2:
            st.markdown(f"### {squad2} 🏆")
        else:
            st.markdown(f"### {squad2}")
        
        # Create table data
        table_data = {
            'Metric': ['Position', 'Points', 'Goal Difference', 'Record'],
            'Value': [
                f"{context2['position']}" if context2['position'] else "N/A",
                f"{context2['points']}" if context2['points'] is not None else "N/A",
                f"+{context2['goal_difference']}" if context2['goal_difference'] is not None and context2['goal_difference'] >= 0 else str(context2['goal_difference']) if context2['goal_difference'] is not None else "N/A",
                f"{context2['wins']}W - {context2['draws']}D - {context2['losses']}L" if context2['wins'] is not None else "N/A"
            ]
        }
        
        df2 = pd.DataFrame(table_data)
        st.dataframe(df2, use_container_width=True, hide_index=True)

    # ============================================================================
    # CATEGORY COMPARISON SUMMARY
    # ============================================================================

    summary = comparison.get('summary', {})

    # Calculate key insights
    table_data = extract_category_comparison_table(comparison, timeframe)

    # Find biggest gaps
    gaps = []
    for row in table_data:
        if row['squad1_composite'] and row['squad2_composite']:
            gap = abs(row['squad1_composite'] - row['squad2_composite'])
            leader = squad1 if row['squad1_composite'] > row['squad2_composite'] else squad2
            gaps.append({
                'category': row['category'].replace('_', ' ').title(),
                'gap': gap,
                'leader': leader
            })

    gaps_sorted = sorted(gaps, key=lambda x: x['gap'], reverse=True)
    biggest_gap = gaps_sorted[0] if gaps_sorted else None

    # Key insight
    if table_data:
        st.markdown("---")
        st.markdown("### Key Insights")
        
        # Find top 3 gaps
        gaps_sorted_top3 = gaps_sorted[:3] if len(gaps_sorted) >= 3 else gaps_sorted
        
        col1, col2, col3 = st.columns(3)
        
        for idx, gap_info in enumerate(gaps_sorted_top3):
            with [col1, col2, col3][idx]:
                st.markdown(f"""
                <div class="insight-box">
                <strong>{gap_info['category']}</strong><br>
                {gap_info['leader']} leads by <strong>{gap_info['gap']:.1f}</strong> points
                </div>
                """, unsafe_allow_html=True)


    # ============================================================================
    # MAIN COMPARISON - RADAR + TABLE SIDE BY SIDE
    # ============================================================================

    st.markdown('<div class="section-header">Performance Comparison</div>', unsafe_allow_html=True)

    # Extract radar data
    categories1, values1 = extract_radar_data(profile1, use_composite=True)
    categories2, values2 = extract_radar_data(profile2, use_composite=True)

    # Create two columns for radar and table
    col_left, col_right = st.columns([1, 1])

    with col_left:
        st.subheader("Radar Chart")
        st.caption("Composite scores across 9 categories")
        
        # Create and display radar chart
        radar_fig = create_radar_chart(
            squad1, categories1, values1,
            squad2, categories2, values2
        )
        st.plotly_chart(radar_fig, use_container_width=True)

    with col_right:
        st.subheader("Category Breakdown")
        st.caption("Composite Scores (0-100) and Ranks (1-20)")
        
        # Create category table with actual squad names and styling
        table_data = extract_category_comparison_table(comparison, timeframe)
        category_df = create_category_table(squad1, squad2, table_data)
        
        if not category_df.empty:
            # Apply styling to highlight winners
            def highlight_winner(row):
                if row['Winner'] == squad1:
                    return ['background-color: rgba(65, 105, 225, 0.15)'] * len(row)
                elif row['Winner'] == squad2:
                    return ['background-color: rgba(220, 20, 60, 0.15)'] * len(row)
                else:
                    return ['background-color: rgba(128, 128, 128, 0.05)'] * len(row)
            
            styled_df = category_df.style.apply(highlight_winner, axis=1)
            
            st.dataframe(
                styled_df,
                use_container_width=True,
                hide_index=True
            )
        else:
            st.warning("No category comparison data available")


    # ============================================================================
    # DETAILED METRICS - EXPANDABLE DRILL-DOWN
    # ============================================================================

    st.markdown('<div class="section-header">Detailed Metric Breakdown</div>', unsafe_allow_html=True)
    st.caption("Expand any category to see individual metrics")

    # Get all categories
    categories = get_category_names()

    # Create expandable sections for each category
    for category in categories:
        category_display = category.replace('_', ' ').title()
        
        with st.expander(f"📊 {category_display}", expanded=False):
            
            # Load category breakdowns
            try:
                breakdown1 = load_category_breakdown(squad1, category, timeframe)
                breakdown2 = load_category_breakdown(squad2, category, timeframe)
                
                # Check for errors
                if "error" in breakdown1 or "error" in breakdown2:
                    st.warning(f"Could not load data for {category_display}")
                    continue
                
                # Show category description
                st.markdown(f"*{breakdown1.get('description', '')}*")
                st.markdown("---")
                
                # Show composite scores and ranks side by side
                col1, col2, col3 = st.columns([0.25, 0.25, 0.5])
                
                # Extract metrics
                metrics1 = extract_metric_breakdown(breakdown1)
                metrics2 = extract_metric_breakdown(breakdown2)

                with col1:
                    st.markdown(f"**{squad1}**")
                    st.metric("Composite Score", f"{breakdown1.get('composite_score', 0):.1f}/100")
                    st.metric("League Rank", f"{breakdown1.get('rank', 'N/A')}/20")
                
                with col2:
                    st.markdown(f"**{squad2}**")
                    st.metric("Composite Score", f"{breakdown2.get('composite_score', 0):.1f}/100")
                    st.metric("League Rank", f"{breakdown2.get('rank', 'N/A')}/20")
                
                with col3:                
                    if metrics1 and metrics2:
                        # Calculate metric gaps
                        metric2_lookup = {m['metric']: m for m in metrics2}
                        metric_gaps = []
                        
                        for m1 in metrics1:
                            m2 = metric2_lookup.get(m1['metric'])
                            if m2 and m1.get('rank') and m2.get('rank'):
                                gap = abs(m1['rank'] - m2['rank'])
                                if m1['rank'] < m2['rank']:
                                    leader = squad1
                                    leader_rank = m1['rank']
                                else:
                                    leader = squad2
                                    leader_rank = m2['rank']
                                
                                metric_gaps.append({
                                    'metric': m1['metric'].replace('_', ' ').title(),
                                    'gap': gap,
                                    'leader': leader,
                                    'leader_rank': leader_rank
                                })
                        
                        # Show top 3 gaps
                        metric_gaps_sorted = sorted(metric_gaps, key=lambda x: x['gap'], reverse=True)[:3]
                        
                        if metric_gaps_sorted:
                            st.markdown("**Biggest Gaps:**")
                            for mg in metric_gaps_sorted:
                                st.caption(f"• {mg['metric']}: {mg['leader']} leads (#{mg['leader_rank']})")
                
                # Full metric comparison table
                st.markdown("---")
                st.markdown("**All Metrics:**")
                
                drill_table = create_metric_drilldown_table(
                    squad1, metrics1,
                    squad2, metrics2
                )
                
                if not drill_table.empty:
                    # Apply styling to highlight metric winners
                    def highlight_metric_winner(row):
                        if row['Winner'] == squad1:
                            return ['background-color: rgba(65, 105, 225, 0.1)'] * len(row)
                        elif row['Winner'] == squad2:
                            return ['background-color: rgba(220, 20, 60, 0.1)'] * len(row)
                        elif row['Winner'] == "Tie":
                            return ['background-color: rgba(128, 128, 128, 0.05)'] * len(row)
                        else:
                            return [''] * len(row)
                    
                    styled_drill = drill_table.style.apply(highlight_metric_winner, axis=1)
                    
                    st.dataframe(
                        styled_drill,
                        use_container_width=True,
                        hide_index=True,
                        height=400
                    )
                else:
                    st.info("No metric data available")
                    
            except Exception as e:
                st.error(f"Error loading {category_display}: {e}")

# ============================================================================
# MAIN APP ROUTING - NEW
# ============================================================================

if page == "League Overview":
    show_league_overview()
elif page == "Squad Profiles":
    show_squad_profile(selected_squad, timeframe)
elif page == "Player Overview":
    show_player_overview(timeframe)
elif page == "Player Analysis":
    show_player_analysis(timeframe, selected_player, min_sim, same_pos)