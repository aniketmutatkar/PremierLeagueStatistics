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

# ============================================================================
# PLAYER VISUALIZATION FUNCTIONS
# ============================================================================

def create_player_dual_radar_chart(player_name, categories, overall_scores, position_scores):
    """
    Create professional dual percentile radar chart for a player
    Shows ONLY position percentiles with 50% baseline reference
    Matches the styling of create_radar_chart()
    """
    import plotly.graph_objects as go
    
    if not categories or not position_scores:
        return None

    # Prepare readable labels and closed loops for radar
    readable_categories = [cat.replace('_', ' ').title() for cat in categories]
    position_scores_closed = position_scores + [position_scores[0]]
    categories_closed = readable_categories + [readable_categories[0]]
    
    # Create 50% baseline for reference
    baseline_50 = [50] * len(categories_closed)

    fig = go.Figure()
    
    # Add 50% baseline first (GREEN, dashed line) - CHANGED FROM GRAY
    fig.add_trace(go.Scatterpolar(
        r=baseline_50,
        theta=categories_closed,
        mode='lines',
        name='Average (50%)',
        line=dict(color='rgba(34, 197, 94, 0.6)', width=4, dash='dash'),  # GREEN
        hoverinfo='skip',
        showlegend=True
    ))

    # Position percentile trace (YOUR DESIGN - Royal Blue #4169E1)
    fig.add_trace(go.Scatterpolar(
        r=position_scores_closed,
        theta=categories_closed,
        fill='toself',
        name=f'{player_name} (Position %ile)',
        line=dict(color='#4169E1', width=3),
        fillcolor='rgba(65, 105, 225, 0.15)',
        hovertemplate='<b>%{theta}</b><br>Percentile: %{r:.1f}<extra></extra>'
    ))

    # Layout styling (EXACTLY matches your create_radar_chart)
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
            y=-0.25,
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


def create_player_category_table(category_data):
    """
    Create category breakdown table with dual percentiles
    
    Args:
        category_data: List of dicts from extract_player_category_table_data()
        
    Returns:
        Styled DataFrame for st.dataframe()
    """
    import pandas as pd
    
    if not category_data:
        return None
    
    # Create DataFrame
    df = pd.DataFrame(category_data)
    
    # Rename columns for display
    display_df = df.copy()
    display_df['category'] = display_df['category'].str.replace('_', ' ').str.title()
    
    display_df = display_df.rename(columns={
        'category': 'Category',
        'overall_score': 'Overall',
        'position_score': 'Position',
        'metrics_analyzed': 'Metrics'
    })
    
    # Select and reorder columns
    display_df = display_df[['Category', 'Overall', 'Position', 'Metrics']]
    
    # Apply styling
    def style_percentiles(val):
        """Color code percentile values"""
        if pd.isna(val):
            return 'color: #999;'
        if val >= 80:
            return 'color: #16a34a; font-weight: bold;'
        elif val >= 60:
            return 'color: #65a30d;'
        elif val >= 40:
            return 'color: #eab308;'
        elif val >= 20:
            return 'color: #f97316;'
        else:
            return 'color: #dc2626;'
    
    styled_df = display_df.style.format({
        'Overall': lambda x: f"{x:.1f}" if pd.notna(x) else "—",
        'Position': lambda x: f"{x:.1f}" if pd.notna(x) else "—"
    }).applymap(style_percentiles, subset=['Overall', 'Position'])
    
    return styled_df


def create_player_comparison_radar(player1_name, categories1, scores1, player2_name, categories2, scores2):
    """
    Create comparison radar chart for two players
    Shows ONLY position percentiles with 50% baseline
    Matches the styling of create_radar_chart()
    """
    import plotly.graph_objects as go

    if not categories1 or not scores1 or not categories2 or not scores2:
        return None

    # Use player1's categories as the base
    readable_categories = [cat.replace('_', ' ').title() for cat in categories1]

    # Close the radar chart loop
    scores1_closed = scores1 + [scores1[0]]
    scores2_closed = scores2 + [scores2[0]]
    categories_closed = readable_categories + [readable_categories[0]]
    
    # Create 50% baseline for reference
    baseline_50 = [50] * len(categories_closed)

    fig = go.Figure()
    
    # Add 50% baseline first (GREEN, dashed line) - CHANGED FROM GRAY
    fig.add_trace(go.Scatterpolar(
        r=baseline_50,
        theta=categories_closed,
        mode='lines',
        name='Average (50%)',
        line=dict(color='rgba(34, 197, 94, 0.6)', width=1, dash='dash'),  # GREEN
        hoverinfo='skip',
        showlegend=True
    ))

    # Player 1 trace (YOUR DESIGN - Royal Blue #4169E1)
    fig.add_trace(go.Scatterpolar(
        r=scores1_closed,
        theta=categories_closed,
        fill='toself',
        name=player1_name,
        line=dict(color='#4169E1', width=3),
        fillcolor='rgba(65, 105, 225, 0.15)',
        hovertemplate='<b>%{theta}</b><br>Percentile: %{r:.1f}<extra></extra>'
    ))

    # Player 2 trace (YOUR DESIGN - Crimson #DC143C)
    fig.add_trace(go.Scatterpolar(
        r=scores2_closed,
        theta=categories_closed,
        fill='toself',
        name=player2_name,
        line=dict(color='#DC143C', width=3),
        fillcolor='rgba(220, 20, 60, 0.15)',
        hovertemplate='<b>%{theta}</b><br>Percentile: %{r:.1f}<extra></extra>'
    ))

    # Layout styling (EXACTLY matches your create_radar_chart)
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
            y=-0.25,
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


def create_player_comparison_table(player1, player2, category_comparison):
    """
    Create category comparison table for two players
    
    Args:
        player1: First player name
        player2: Second player name
        category_comparison: Dict from load_player_comparison()
        
    Returns:
        Styled DataFrame for st.dataframe()
    """
    import pandas as pd
    
    if not category_comparison:
        return None
    
    # Build comparison data
    table_data = []
    
    for category, data in category_comparison.items():
        score1 = data.get(f'{player1}_score', 0)
        score2 = data.get(f'{player2}_score', 0)
        winner = data.get('winner', 'tie')
        
        # Determine winner symbol
        if winner == player1:
            winner_symbol = player1.split()[-1]  # get last word
        elif winner == player2:
            winner_symbol = player2.split()[-1]
        else:
            winner_symbol = '—'
        
        table_data.append({
            'Category': category.replace('_', ' ').title(),
            player1.split()[-1]: f"{score1:.1f}" if score1 is not None else "—",
            player2.split()[-1]: f"{score2:.1f}" if score2 is not None else "—",
            'Winner': winner_symbol
        })
    
    df = pd.DataFrame(table_data)
    
    # Style the table
    def highlight_winner(row):
        """Highlight winner's score in their color"""
        styles = [''] * len(row)
        
        if row['Winner'] == player1.split()[-1]:
            styles[1] = 'background-color: rgba(59, 130, 246, 0.2); font-weight: bold;'
        elif row['Winner'] == player2.split()[-1]:
            styles[2] = 'background-color: rgba(239, 68, 68, 0.2); font-weight: bold;'
        
        return styles
    
    styled_df = df.style.apply(highlight_winner, axis=1)
    
    return styled_df


def create_similar_players_table(similar_players_data):
    """
    Create table showing similar players with similarity scores
    
    Args:
        similar_players_data: Output from load_similar_players()
        
    Returns:
        Styled DataFrame for st.dataframe()
    """
    import pandas as pd
    
    if "error" in similar_players_data:
        return None
    
    similar_players = similar_players_data.get('similar_players', [])
    
    if not similar_players:
        return None
    
    # Build table data
    table_data = []
    
    for i, player in enumerate(similar_players, 1):
        similarity = player['similarity_score']
        
        # Generate star rating
        if similarity >= 90:
            stars = "★★★★★"
        elif similarity >= 85:
            stars = "★★★★☆"
        elif similarity >= 80:
            stars = "★★★☆☆"
        elif similarity >= 75:
            stars = "★★☆☆☆"
        else:
            stars = "★☆☆☆☆"
        
        table_data.append({
            'Rank': i,
            'Player': player['player_name'],
            'Position': player['position'],
            'Similarity': f"{similarity:.1f}%",
            'Rating': stars
        })
    
    df = pd.DataFrame(table_data)
    
    # Apply color gradient to similarity scores
    def color_similarity(val):
        """Color code similarity percentage"""
        # Extract numeric value
        pct = float(val.rstrip('%'))
        
        if pct >= 90:
            return 'color: #16a34a; font-weight: bold;'
        elif pct >= 85:
            return 'color: #65a30d; font-weight: bold;'
        elif pct >= 80:
            return 'color: #eab308;'
        elif pct >= 75:
            return 'color: #f97316;'
        else:
            return 'color: #dc2626;'
    
    styled_df = df.style.applymap(color_similarity, subset=['Similarity'])
    
    return styled_df

# ============================================================================
# PLAYER OVERVIEW VISUALIZATION FUNCTIONS - ADD TO dashboard/charts.py
# ============================================================================

def create_player_rankings_table(df):
    """
    Create player rankings table for Player Overview
    
    Args:
        df: DataFrame from load_player_overview()
        
    Returns:
        Styled dataframe for st.dataframe()
    """
    if df.empty:
        return df
    
    # Make a copy
    display_df = df.copy()
    
    # Add rank column
    display_df.insert(0, 'rank', range(1, len(display_df) + 1))
    
    # Select columns for display
    table_cols = ['rank', 'player_name', 'position', 'squad', 'minutes_played', 'overall_score']
    display_df = display_df[table_cols]
    
    # Rename columns
    display_df = display_df.rename(columns={
        'rank': 'Rank',
        'player_name': 'Player',
        'position': 'Pos',
        'squad': 'Squad',
        'minutes_played': 'Minutes',
        'overall_score': 'Overall'
    })
    
    # Format columns
    display_df['Minutes'] = display_df['Minutes'].apply(lambda x: f"{x:,}" if pd.notna(x) else "—")
    display_df['Overall'] = display_df['Overall'].apply(lambda x: f"{x:.1f}" if pd.notna(x) else "—")
    
    return display_df

def create_player_category_heatmap(df, sort_category=None, position_filter=None):
    """
    Create heatmap showing top 10 players for a specific position across 8 OUTFIELD categories + overall score
    FOR OUTFIELD PLAYERS ONLY (FW, MF, DF)
    """
    import plotly.graph_objects as go
    import numpy as np
    
    if df.empty or position_filter is None:
        return go.Figure()
    
    pos_df = df[df['primary_position'] == position_filter].copy()
    
    if pos_df.empty:
        return go.Figure()
    
    # Sort DESCENDING (highest scores first)
    if sort_category and f"{sort_category}_pos" in pos_df.columns:
        pos_df = pos_df.sort_values(f"{sort_category}_pos", ascending=False, na_position='last')
    else:
        pos_df = pos_df.sort_values('overall_score', ascending=False, na_position='last')
    
    # REVERSE the order so #1 is at TOP of heatmap
    top_players_df = pos_df.head(10)
    top_players_df = top_players_df.iloc[::-1]
    
    if top_players_df.empty:
        return go.Figure()
    
    player_names = top_players_df['player_name'].tolist()
    
    # Get OUTFIELD category columns (8 categories) + overall score
    outfield_categories = [
        'attacking_output_pos', 'creativity_pos', 'passing_pos', 
        'ball_progression_pos', 'defending_pos', 'physical_duels_pos', 
        'ball_involvement_pos', 'discipline_reliability_pos'
    ]
    
    # Filter to only outfield categories that exist in the data
    category_cols = [col for col in outfield_categories if col in top_players_df.columns]
    
    # Add overall_score to the display
    all_cols = ['overall_score'] + category_cols
    
    # Clean category names for display
    category_labels = ['Overall'] + [col.replace('_pos', '').replace('_', ' ').title() for col in category_cols]
    
    # Extract values as 2D array
    z_values = top_players_df[all_cols].values
    
    # Calculate ranks within this position for each category
    rank_values = np.zeros_like(z_values, dtype=int)
    
    # First column is overall_score - calculate its rank
    overall_scores = pos_df['overall_score'].values
    valid_overall = ~np.isnan(overall_scores)
    if valid_overall.sum() > 0:
        overall_ranks = pd.Series(overall_scores).rank(method='min', ascending=False, na_option='keep').values.astype(int)
        
        for i, player_name in enumerate(player_names):
            player_idx_in_full = pos_df[pos_df['player_name'] == player_name].index[0]
            full_df_loc = pos_df.index.get_loc(player_idx_in_full)
            rank_values[i, 0] = overall_ranks[full_df_loc] if not np.isnan(overall_scores[full_df_loc]) else 0
    
    # Now do category columns (starting from index 1)
    for j, category_col in enumerate(category_cols, start=1):
        category_scores = pos_df[category_col].values
        valid_scores = ~np.isnan(category_scores)
        if valid_scores.sum() > 0:
            ranks = pd.Series(category_scores).rank(method='min', ascending=False, na_option='keep').values.astype(int)
            
            for i, player_name in enumerate(player_names):
                player_idx_in_full = pos_df[pos_df['player_name'] == player_name].index[0]
                full_df_loc = pos_df.index.get_loc(player_idx_in_full)
                rank_values[i, j] = ranks[full_df_loc] if not np.isnan(category_scores[full_df_loc]) else 0
    
    # Create text overlay with ranks
    text_values = []
    for i in range(len(player_names)):
        row_text = []
        for j in range(len(all_cols)):
            rank = rank_values[i, j]
            if rank > 0:
                row_text.append(f"#{rank}")
            else:
                row_text.append("")
        text_values.append(row_text)
    
    # Create hover text with full details
    hover_text = []
    for i, player_name in enumerate(player_names):
        row_hover = []
        for j, category in enumerate(category_labels):
            score = z_values[i][j]
            rank = rank_values[i, j]
            if not np.isnan(score) and rank > 0:
                text = f"{player_name}<br>{category}<br>Score: {score:.1f}<br>Rank: #{rank}/{len(pos_df)}"
            else:
                text = f"{player_name}<br>{category}<br>No Data"
            row_hover.append(text)
        hover_text.append(row_hover)
    
    fig = go.Figure(data=go.Heatmap(
        z=z_values,
        x=category_labels,
        y=player_names,
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
            title="Percentile",
            titleside="right",
            tickmode="linear",
            tick0=0,
            dtick=20
        )
    ))
    
    fig.update_layout(
        xaxis_title="",
        yaxis_title="",
        height=500,
        xaxis=dict(side='top', tickangle=20),
        font=dict(size=11),
        margin=dict(t=10, b=10, l=150, r=10),
        showlegend=False
    )
    
    return fig

def create_goalkeeper_heatmap(df, sort_category=None):
    """
    Create heatmap showing top 10 goalkeepers with GOALKEEPER-SPECIFIC categories
    
    Args:
        df: DataFrame from load_player_overview()
        sort_category: Optional GK category to sort by (e.g., 'shot_stopping')
        
    Returns:
        Plotly figure for st.plotly_chart()
    """
    import plotly.graph_objects as go
    import numpy as np
    
    if df.empty:
        return go.Figure()
    
    # Filter to goalkeepers only
    gk_df = df[df['primary_position'] == 'GK'].copy()
    
    if gk_df.empty:
        return go.Figure()
    
    # For GKs, we need to load their profiles to get GK categories
    # Since GK categories aren't in the overview df, we need to fetch them
    
    from data_loader import load_player_profile
    
    gk_data = []
    for _, row in gk_df.head(10).iterrows():  # Top 10 by overall score
        player_name = row['player_name']
        profile = load_player_profile(player_name, 'current')  # You'll need to pass timeframe
        
        if 'error' not in profile:
            gk_categories = profile['dual_percentiles']['category_scores']
            
            # Extract GK category scores
            category_scores = {}
            for cat_name, cat_data in gk_categories.items():
                pos_score = cat_data.get('position_score')
                category_scores[cat_name] = pos_score if pos_score is not None else 0
            
            gk_data.append({
                'player_name': player_name,
                'overall_score': row['overall_score'],
                **category_scores
            })
    
    if not gk_data:
        return go.Figure()
    
    gk_heatmap_df = pd.DataFrame(gk_data)
    
    # Sort if sort_category specified
    if sort_category and sort_category in gk_heatmap_df.columns:
        gk_heatmap_df = gk_heatmap_df.sort_values(sort_category, ascending=False)
    else:
        gk_heatmap_df = gk_heatmap_df.sort_values('overall_score', ascending=False)
    
    # Reverse so #1 is at top
    gk_heatmap_df = gk_heatmap_df.iloc[::-1]
    
    player_names = gk_heatmap_df['player_name'].tolist()
    
    # Get GK category columns (exclude overall_score and player_name)
    gk_category_cols = [col for col in gk_heatmap_df.columns if col not in ['player_name', 'overall_score']]
    
    # Add overall_score as first column
    all_cols = ['overall_score'] + gk_category_cols
    category_labels = ['Overall'] + [col.replace('_', ' ').title() for col in gk_category_cols]
    
    # Extract values
    z_values = gk_heatmap_df[all_cols].values
    
    # Calculate ranks (all GKs ranked against each other)
    rank_values = np.zeros_like(z_values, dtype=int)
    
    for j, col in enumerate(all_cols):
        col_values = gk_heatmap_df[col].values
        valid = ~np.isnan(col_values)
        if valid.sum() > 0:
            ranks = pd.Series(col_values).rank(method='min', ascending=False, na_option='keep').values.astype(int)
            rank_values[:, j] = ranks
    
    # Create text overlay
    text_values = []
    for i in range(len(player_names)):
        row_text = []
        for j in range(len(all_cols)):
            rank = rank_values[i, j]
            if rank > 0:
                row_text.append(f"#{rank}")
            else:
                row_text.append("")
        text_values.append(row_text)
    
    # Create hover text
    hover_text = []
    for i, player_name in enumerate(player_names):
        row_hover = []
        for j, category in enumerate(category_labels):
            score = z_values[i][j]
            rank = rank_values[i, j]
            if not np.isnan(score) and rank > 0:
                text = f"{player_name}<br>{category}<br>Score: {score:.1f}<br>Rank: #{rank}/10"
            else:
                text = f"{player_name}<br>{category}<br>No Data"
            row_hover.append(text)
        hover_text.append(row_hover)
    
    fig = go.Figure(data=go.Heatmap(
        z=z_values,
        x=category_labels,
        y=player_names,
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
            title="Percentile",
            titleside="right",
            tickmode="linear",
            tick0=0,
            dtick=20
        )
    ))
    
    fig.update_layout(
        xaxis_title="",
        yaxis_title="",
        height=500,
        xaxis=dict(side='top', tickangle=20),
        font=dict(size=11),
        margin=dict(t=10, b=10, l=150, r=10),
        showlegend=False
    )
    
    return fig


def create_player_category_leaderboard_table(df, category):
    """
    Create simple top 10 leaderboard for a category
    
    Args:
        df: DataFrame from load_player_category_leaderboard()
        category: Category name for display
        
    Returns:
        Styled dataframe
    """
    if df.empty:
        return df
    
    # Make a copy
    display_df = df.copy()
    
    # Rename columns for display
    display_df = display_df.rename(columns={
        'rank': 'Rank',
        'player_name': 'Player',
        'position': 'Pos',
        'squad': 'Squad',
        'score': 'Score'
    })
    
    # Format score to 1 decimal
    display_df['Score'] = display_df['Score'].apply(
        lambda x: f"{x:.1f}" if pd.notna(x) else "—"
    )
    
    return display_df


def create_squad_dominance_charts(df, top_n_players=100):
    """
    Create squad dominance chart with position breakdown + squad average as text
    LEFT: Player count by position (stacked bar)
    RIGHT: Just text showing squad average score
    """
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    
    if df.empty or 'overall_score' not in df.columns:
        return go.Figure()
    
    # Get top N players by overall score
    top_players = df.nlargest(top_n_players, 'overall_score')
    
    if top_players.empty:
        return go.Figure()
    
    # Group by squad and position
    squad_position_counts = top_players.groupby(['squad', 'primary_position']).size().unstack(fill_value=0)
    squad_position_counts['Total'] = squad_position_counts.sum(axis=1)
    squad_position_counts = squad_position_counts.sort_values('Total', ascending=True).tail(15)
    
    # Calculate OVERALL squad average (all players, not just elite)
    squad_avg_all = df.groupby('squad')['overall_score'].mean()
    squad_avg_all = squad_avg_all[squad_position_counts.index]
    
    # Create figure with subplots
    fig = make_subplots(
        rows=1, cols=2,
        column_widths=[0.75, 0.25],
        subplot_titles=(f'Elite Player Count (Top {top_n_players})', 'Squad Avg'),
        horizontal_spacing=0.02
    )
    
    # ========================================================================
    # LEFT: Stacked position bars (DON'T TOUCH)
    # ========================================================================
    
    positions = ['FW', 'MF', 'DF', 'GK']
    colors = {'FW': '#DC143C', 'MF': '#4169E1', 'DF': '#2E8B57', 'GK': '#FF8C00'}
    
    for pos in positions:
        if pos in squad_position_counts.columns:
            fig.add_trace(go.Bar(
                name=pos,
                y=squad_position_counts.index,
                x=squad_position_counts[pos],
                orientation='h',
                marker=dict(color=colors.get(pos, '#666666')),
                text=squad_position_counts[pos].apply(lambda x: str(int(x)) if x > 0 else ''),
                textposition='inside',
                textfont=dict(color='white', size=10),
                hovertemplate='<b>%{y}</b><br>' + pos + ': %{x}<extra></extra>',
                showlegend=True
            ), row=1, col=1)
    
    # ========================================================================
    # RIGHT: Just text annotations (no bars)
    # ========================================================================
    
    for i, (squad, avg_score) in enumerate(squad_avg_all.items()):
        fig.add_annotation(
            x=0.5,
            y=i,
            text=f"{avg_score:.1f}",
            xref='x2',
            yref='y2',
            showarrow=False,
            font=dict(size=14, color='white'),
            xanchor='center'
        )
    
    # Update layout
    fig.update_layout(
        barmode='stack',
        height=500,
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="right", x=0.75),
        margin=dict(t=80, b=40, l=120, r=20)
    )
    
    # Update axes
    fig.update_xaxes(title_text="Players", row=1, col=1)
    fig.update_xaxes(visible=False, row=1, col=2)
    fig.update_yaxes(showticklabels=True, row=1, col=1)
    fig.update_yaxes(showticklabels=False, row=1, col=2)
    
    return fig