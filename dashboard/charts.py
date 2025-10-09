#!/usr/bin/env python3
"""
Chart Components for Squad Analytics Dashboard
===============================================

Creates clean, professional visualizations.
"""

import plotly.graph_objects as go
import pandas as pd
import numpy as np


# ============================================================================
# RADAR CHART
# ============================================================================

def create_radar_chart(squad1_name, squad1_categories, squad1_values,
                       squad2_name, squad2_categories, squad2_values):
    """
    Create professional radar chart with clean styling
    """
    
    fig = go.Figure()
    
    # Squad 1 trace
    fig.add_trace(go.Scatterpolar(
        r=squad1_values,
        theta=[cat.replace('_', ' ').title() for cat in squad1_categories],
        fill='toself',
        name=squad1_name,
        line=dict(color='#4169E1', width=3),
        fillcolor='rgba(65, 105, 225, 0.15)',
        hovertemplate='<b>%{theta}</b><br>Score: %{r:.1f}<extra></extra>'
    ))
    
    # Squad 2 trace
    fig.add_trace(go.Scatterpolar(
        r=squad2_values,
        theta=[cat.replace('_', ' ').title() for cat in squad2_categories],
        fill='toself',
        name=squad2_name,
        line=dict(color='#DC143C', width=3),
        fillcolor='rgba(220, 20, 60, 0.15)',
        hovertemplate='<b>%{theta}</b><br>Score: %{r:.1f}<extra></extra>'
    ))
    
    # Layout
    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 100],
                tickmode='linear',
                tick0=0,
                dtick=25,
                showline=False,
                gridcolor='rgba(128, 128, 128, 0.2)',
                tickfont=dict(size=10)
            ),
            angularaxis=dict(
                linewidth=2,
                showline=True,
                gridcolor='rgba(128, 128, 128, 0.2)',
                tickfont=dict(size=11)
            ),
            bgcolor='rgba(0,0,0,0)'
        ),
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.2,
            xanchor="center",
            x=0.5,
            font=dict(size=12)
        ),
        height=450,
        margin=dict(l=80, r=80, t=40, b=100),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)'
    )
    
    return fig


# ============================================================================
# CATEGORY COMPARISON TABLE
# ============================================================================

def create_category_table(squad1_name, squad2_name, comparison_data):
    """
    Create clean comparison table showing BOTH composite and rank
    """
    
    if not comparison_data:
        return pd.DataFrame()
    
    # Build rows with BOTH composite and rank
    rows = []
    for row in comparison_data:
        category = row['category'].replace('_', ' ').title()
        
        # Format: "72.4 (Rank 2/20)"
        squad1_comp = row['squad1_composite'] if row['squad1_composite'] is not None else 0
        squad1_rank = row['squad1_rank'] if row['squad1_rank'] is not None else 'N/A'
        squad1_display = f"{squad1_comp:.1f} (#{squad1_rank})"
        
        squad2_comp = row['squad2_composite'] if row['squad2_composite'] is not None else 0
        squad2_rank = row['squad2_rank'] if row['squad2_rank'] is not None else 'N/A'
        squad2_display = f"{squad2_comp:.1f} (#{squad2_rank})"
        
        rows.append({
            'Category': category,
            squad1_name: squad1_display,
            squad2_name: squad2_display,
            'Winner': row['winner']
        })
    
    df = pd.DataFrame(rows)
    
    # Sort: Winners first
    def sort_key(row):
        if row['Winner'] == squad1_name:
            return 0
        elif row['Winner'] == squad2_name:
            return 1
        else:
            return 2
    
    df['sort_order'] = df.apply(sort_key, axis=1)
    df = df.sort_values('sort_order').drop('sort_order', axis=1)
    
    return df

# ============================================================================
# METRIC DRILL-DOWN TABLE
# ============================================================================

def create_metric_drilldown_table(squad1_name, squad1_metrics, 
                                   squad2_name, squad2_metrics):
    """
    Create metric comparison with Value AND Rank
    Shows ALL metrics from both squads
    """
    
    if not squad1_metrics or not squad2_metrics:
        return pd.DataFrame()
    
    # Create lookups for both squads
    squad1_lookup = {m['metric']: m for m in squad1_metrics}
    squad2_lookup = {m['metric']: m for m in squad2_metrics}
    
    # Get ALL unique metrics from both squads
    all_metrics = set()
    for m in squad1_metrics:
        all_metrics.add(m['metric'])
    for m in squad2_metrics:
        all_metrics.add(m['metric'])
    
    # Sort metrics alphabetically for consistency
    all_metrics = sorted(list(all_metrics))
    
    rows = []
    for metric in all_metrics:
        metric_name = metric.replace('_', ' ').title()
        
        # Get data for both squads
        m1 = squad1_lookup.get(metric, {})
        m2 = squad2_lookup.get(metric, {})
        
        # Format squad 1: "26 (#1/20)"
        if m1.get('value') is not None:
            val1 = f"{m1['value']:.1f}" if isinstance(m1['value'], (int, float)) else str(m1['value'])
        else:
            val1 = "—"
        
        rank1 = m1.get('rank')
        if rank1 is not None:
            # Convert to int if string
            if isinstance(rank1, str):
                rank1 = int(rank1)
            rank1_display = f"{rank1}/20"
        else:
            rank1_display = "—"
        
        display1 = f"{val1} (#{rank1_display})"
        
        # Format squad 2: "23 (#4/20)"
        if m2.get('value') is not None:
            val2 = f"{m2['value']:.1f}" if isinstance(m2['value'], (int, float)) else str(m2['value'])
        else:
            val2 = "—"
        
        rank2 = m2.get('rank')
        if rank2 is not None:
            # Convert to int if string
            if isinstance(rank2, str):
                rank2 = int(rank2)
            rank2_display = f"{rank2}/20"
        else:
            rank2_display = "—"
        
        display2 = f"{val2} (#{rank2_display})"
        
        # Winner based on rank (lower = better)
        if rank1 is not None and rank2 is not None:
            # Ensure both are ints
            r1 = int(rank1) if isinstance(rank1, str) else rank1
            r2 = int(rank2) if isinstance(rank2, str) else rank2
            
            if r1 < r2:
                winner = squad1_name
            elif r2 < r1:
                winner = squad2_name
            else:
                winner = "Tie"
        else:
            winner = "—"
        
        rows.append({
            'Metric': metric_name,
            squad1_name: display1,
            squad2_name: display2,
            'Winner': winner
        })
    
    df = pd.DataFrame(rows)
    
    # Sort by winner first, then by metric name
    def sort_key(row):
        if row['Winner'] == squad1_name:
            return (0, row['Metric'])
        elif row['Winner'] == squad2_name:
            return (2, row['Metric'])
        elif row['Winner'] == "Tie":
            return (1, row['Metric'])
        else:
            return (3, row['Metric'])
    
    df['sort_order'] = df.apply(lambda row: sort_key(row)[0], axis=1)
    df = df.sort_values(['sort_order', 'Metric']).drop('sort_order', axis=1)
    
    return df


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def extract_basic_info(squad_profile):
    """Extract basic squad info for display"""
    if "error" in squad_profile:
        return {}
    
    basic = squad_profile.get('basic_info', {})
    
    return {
        'squad_name': basic.get('squad_name', 'N/A'),
        'season': basic.get('season', 'N/A'),
        'gameweek': basic.get('gameweek', 'N/A'),
        'matches_played': basic.get('matches_played', 0),
        'minutes_played': basic.get('minutes_played', 0)
    }

def create_league_table(df):
    """
    Create clean league table with traditional stats + overall composite ONLY
    NO individual category scores - those are in the heatmap
    
    Args:
        df: DataFrame from load_league_overview()
        
    Returns:
        Styled dataframe for st.dataframe()
    """
    # Make a copy to avoid modifying original
    display_df = df.copy()
    
    # Select only traditional columns + overall composite
    table_cols = ['position', 'squad_name', 'points', 'goal_difference', 'wins', 'draws', 'losses', 'overall_composite']
    display_df = display_df[table_cols]
    
    # Rename columns
    display_df = display_df.rename(columns={
        'position': 'Pos',
        'squad_name': 'Squad',
        'points': 'Pts',
        'goal_difference': 'GD',
        'wins': 'W',
        'draws': 'D',
        'losses': 'L',
        'overall_composite': 'Overall'
    })
    
    # Format overall score to 1 decimal
    styled_df = display_df.style.format({'Overall': '{:.1f}'}, na_rep="—")
    
    return styled_df


def create_category_heatmap(df):
    """
    Create heatmap showing all squads across all categories
    WITH rank numbers overlaid on the colored cells
    
    Args:
        df: DataFrame from load_league_overview()
        
    Returns:
        Plotly figure for st.plotly_chart()
    """
    import plotly.graph_objects as go
    
    # Sort by position (1st place at top)
    df_sorted = df.sort_values('position').reset_index(drop=True)
    
    # Extract squad names
    squad_names = df_sorted['squad_name'].tolist()
    
    # Get actual category columns (exclude traditional stats and overall)
    traditional_cols = ['position', 'squad_name', 'points', 'goal_difference', 'wins', 'draws', 'losses', 'overall_composite']
    category_cols = [col for col in df.columns if col not in traditional_cols]
    
    # Clean category names for display
    category_labels = [col.replace('_', ' ').title() for col in category_cols]
    
    # Extract composite scores as 2D array (squads x categories)
    z_values = df_sorted[category_cols].values
    
    # Calculate ranks for each category
    # For each category, rank squads (1 = best, 20 = worst)
    rank_values = np.zeros_like(z_values, dtype=int)
    for j, category in enumerate(category_cols):
        # Get all squads' scores for this category
        category_scores = df[category].values
        # Rank them (descending - higher score = better rank)
        ranks = pd.Series(category_scores).rank(method='min', ascending=False).astype(int).values
        # Map back to sorted order
        for i, squad in enumerate(squad_names):
            squad_idx = df[df['squad_name'] == squad].index[0]
            rank_values[i, j] = ranks[squad_idx]
    
    # Create text overlay with ranks
    text_values = []
    for i in range(len(squad_names)):
        row_text = []
        for j in range(len(category_cols)):
            rank = rank_values[i, j]
            row_text.append(f"#{rank}")
        text_values.append(row_text)
    
    # Create hover text with full details
    hover_text = []
    for i, squad in enumerate(squad_names):
        row_hover = []
        for j, category in enumerate(category_labels):
            score = z_values[i][j]
            rank = rank_values[i][j]
            if pd.notna(score):
                text = f"{squad}<br>{category}<br>Score: {score:.1f}<br>Rank: #{rank}/20"
            else:
                text = f"{squad}<br>{category}<br>Score: —<br>Rank: —"
            row_hover.append(text)
        hover_text.append(row_hover)
    
    # Create heatmap
    fig = go.Figure(data=go.Heatmap(
        z=z_values,
        x=category_labels,
        y=squad_names,
        colorscale='RdYlGn',
        zmid=50,
        zmin=0,
        zmax=100,
        text=text_values,
        texttemplate='%{text}',
        textfont=dict(size=10, color='black'),
        hovertext=hover_text,
        hovertemplate='%{hovertext}<extra></extra>',
        colorbar=dict(
            title="Composite<br>Score",
            titleside="right",
            tickmode="linear",
            tick0=0,
            dtick=20
        )
    ))
    
    # Update layout
    fig.update_layout(
        title="Category Performance Heatmap (with Ranks)",
        xaxis_title="Category",
        yaxis_title="Squad (by League Position)",
        height=800,
        xaxis=dict(side='top'),
        yaxis=dict(autorange='reversed'),  # 1st place at top
        font=dict(size=11)
    )
    
    return fig

def create_category_leaderboard(df, category):
    """
    Create simple top 5 leaderboard for a category
    
    Args:
        df: DataFrame from load_category_leaderboard()
        category: Category name for display
        
    Returns:
        Styled dataframe
    """
    # Make a copy
    display_df = df.copy()
    
    # Rename columns for display
    display_df = display_df.rename(columns={
        'rank': 'Rank',
        'squad_name': 'Squad',
        'composite_score': 'Score'
    })
    
    # Format score to 1 decimal
    display_df['Score'] = display_df['Score'].apply(
        lambda x: f"{x:.1f}" if pd.notna(x) else "—"
    )
    
    # Return styled dataframe
    return display_df[['Rank', 'Squad', 'Score']]

def create_position_vs_quality_scatter(df):
    """
    Create scatter plot showing league position vs statistical quality
    Reveals over/underperformers
    
    Args:
        df: DataFrame from load_league_overview()
        
    Returns:
        Plotly figure for st.plotly_chart()
    """
    import plotly.graph_objects as go
    import numpy as np
    from scipy import stats
    
    # Extract data
    positions = df['position'].values
    overall_scores = df['overall_composite'].values
    squad_names = df['squad_name'].values
    
    # Calculate trend line (linear regression)
    slope, intercept, r_value, p_value, std_err = stats.linregress(positions, overall_scores)
    trend_line = slope * positions + intercept
    
    # Calculate residuals (distance from trend line)
    residuals = overall_scores - trend_line
    
    # Identify outliers (top 3 over/underperformers)
    top_overperformers_idx = np.argsort(residuals)[-3:]  # Positive residuals = better than expected
    top_underperformers_idx = np.argsort(residuals)[:3]   # Negative residuals = worse than expected
    
    # Store outliers for text summary (we'll return this separately)
    overperformers = [(squad_names[i], positions[i], residuals[i]) for i in top_overperformers_idx]
    underperformers = [(squad_names[i], positions[i], residuals[i]) for i in top_underperformers_idx]
    
    # Color coding by league position zones
    colors = []
    for pos in positions:
        if pos <= 4:
            colors.append('rgb(46, 204, 113)')  # Green - Top 4
        elif pos <= 10:
            colors.append('rgb(241, 196, 15)')  # Yellow/Orange - 5-10
        else:
            colors.append('rgb(231, 76, 60)')   # Red - 11-20
    
    # Create scatter plot
    fig = go.Figure()
    
    # Determine text position based on whether squad is above or below trend line
    text_positions = []
    for i in range(len(residuals)):
        if residuals[i] > 0:
            text_positions.append('top center')  # Above trend line = label on top
        else:
            text_positions.append('bottom center')  # Below trend line = label on bottom
    
    # Add scatter points WITH DYNAMIC TEAM NAMES
    fig.add_trace(go.Scatter(
        x=positions,
        y=overall_scores,
        mode='markers+text',
        marker=dict(
            size=10,
            color=colors,
            line=dict(width=2, color='white')
        ),
        text=squad_names,
        textposition=text_positions,  # CHANGED: Dynamic positioning
        textfont=dict(size=9, color='white'),
        hovertemplate='<b>%{text}</b><br>Position: %{x}<br>Overall Score: %{y:.1f}<extra></extra>',
        showlegend=False
    ))
    
    # Add trend line
    fig.add_trace(go.Scatter(
        x=positions,
        y=trend_line,
        mode='lines',
        line=dict(color='gray', width=2, dash='dash'),
        name='Expected Performance',
        hovertemplate='Expected Score: %{y:.1f}<extra></extra>'
    ))
    
    # Update layout - NO ANNOTATIONS
    fig.update_layout(
        xaxis_title="League Position",
        yaxis_title="Overall Composite Score",
        xaxis=dict(
            autorange='reversed',  # 1st place on left
            tickmode='linear',
            tick0=1,
            dtick=2
        ),
        yaxis=dict(
            range=[overall_scores.min() - 5, overall_scores.max() + 5]
        ),
        height=600,
        hovermode='closest',
        showlegend=True,
        legend=dict(
            yanchor="bottom",
            y=0.02,
            xanchor="right",
            x=0.98
        ),
        margin=dict(t=0, b=0, l=0, r=0)  # More top margin to prevent title cutoff
    )
    
    # Return both the figure AND the outlier data for text summary
    return fig, overperformers, underperformers

def create_category_winners_chart(df):
    """
    Create horizontal bar chart showing the #1 team in each category
    
    Args:
        df: DataFrame from load_league_overview()
        
    Returns:
        Plotly figure for st.plotly_chart()
    """
    import plotly.graph_objects as go
    
    # Get actual category columns (exclude traditional stats and overall)
    traditional_cols = ['position', 'squad_name', 'points', 'goal_difference', 'wins', 'draws', 'losses', 'overall_composite']
    category_cols = [col for col in df.columns if col not in traditional_cols]
    
    # Find the winner (highest score) for each category
    winners_data = []
    
    for category in category_cols:
        # Find squad with highest score in this category
        max_idx = df[category].idxmax()
        winner_squad = df.loc[max_idx, 'squad_name']
        winner_score = df.loc[max_idx, category]
        
        # Clean category name for display
        category_display = category.replace('_', ' ').title()
        
        winners_data.append({
            'category': category_display,
            'squad': winner_squad,
            'score': winner_score
        })
    
    # Sort by score (highest first) for better visual
    winners_data = sorted(winners_data, key=lambda x: x['score'], reverse=True)
    
    # Extract data for plotting
    categories = [w['category'] for w in winners_data]
    squads = [w['squad'] for w in winners_data]
    scores = [w['score'] for w in winners_data]
    
    # Create color map (assign unique color to each squad)
    unique_squads = list(set(squads))
    color_palette = [
        '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
        '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf'
    ]
    squad_colors = {squad: color_palette[i % len(color_palette)] for i, squad in enumerate(unique_squads)}
    bar_colors = [squad_colors[squad] for squad in squads]
    
    # Create horizontal bar chart
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        y=categories,
        x=scores,
        orientation='h',
        marker=dict(
            color=bar_colors,
            line=dict(width=1, color='white')
        ),
        text=[f"{squad} ({score:.1f})" for squad, score in zip(squads, scores)],
        textposition='inside',
        textfont=dict(size=11, color='white'),
        hovertemplate='<b>%{y}</b><br>Winner: %{text}<extra></extra>'
    ))
    
    # Update layout
    fig.update_layout(
        title="Category Leaders (Top Performer in Each Category)",
        xaxis_title="Composite Score",
        yaxis_title="Category",
        height=400,
        xaxis=dict(range=[0, 100]),
        yaxis=dict(autorange='reversed'),  # Highest score at top
        showlegend=False,
        margin=dict(l=150)  # More space for category names
    )
    
    return fig