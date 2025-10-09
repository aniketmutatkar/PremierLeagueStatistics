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
    load_league_overview,              # NEW
    load_category_leaderboard          # NEW
)

from charts import (
    create_radar_chart,
    create_category_table,
    create_metric_drilldown_table,
    extract_basic_info,
    create_league_table,               # NEW
    create_category_heatmap,           # NEW
    create_category_leaderboard,       # NEW
    create_category_winners_chart,
    create_position_vs_quality_scatter
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

# ============================================================================
# PAGE CONFIG
# ============================================================================

st.set_page_config(
    page_title="Squad Analytics",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="collapsed"
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
        ["Squad Comparison", "League Overview"],
        index=0,
        label_visibility="collapsed"  # Hide the "Navigate" label
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
    # SQUAD SELECTORS (ONLY FOR SQUAD COMPARISON PAGE)
    # ========================================================================
    if page == "Squad Comparison":
        st.markdown("---")
        
        squads = get_available_squads(timeframe)
        
        if not squads or len(squads) < 2:
            st.error("Not enough squads available")
            st.stop()
        
        squad1 = st.selectbox(
            "Squad 1",
            squads,
            index=0
        )
        
        squad2 = st.selectbox(
            "Squad 2",
            squads,
            index=1 if len(squads) > 1 else 0
        )
    
    # ========================================================================
    # FOOTER INFO
    # ========================================================================
    st.markdown("---")
    st.caption("💾 Data refreshes after each gameweek")
    st.caption("📊 9 statistical categories analyzed")


# ============================================================================
# LEAGUE OVERVIEW PAGE
# ============================================================================

def show_league_overview():
    """League Overview page with table, scatter plot, heatmap, winners chart, and top 5 rankings"""
    
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<p class="main-title">Premier League Overview</p>', unsafe_allow_html=True)
    st.markdown(f"**Season:** {selected_season}")
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
    
    st.markdown('<div class="section-header">League Standings & Statistical Quality</div>', unsafe_allow_html=True)
    st.markdown("League table with composite scores alongside position vs quality analysis.")
    
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
# SQUAD COMPARISON PAGE (EXISTING CODE - UNCHANGED)
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

    st.markdown(f'<div class="main-title">{squad1} vs {squad2}</div>', unsafe_allow_html=True)
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

elif page == "League Overview":
    show_league_overview()