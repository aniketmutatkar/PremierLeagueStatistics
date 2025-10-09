#!/usr/bin/env python3
"""
Chart Components for Squad Analytics Dashboard
===============================================

Creates clean, professional visualizations.
"""

import plotly.graph_objects as go
import pandas as pd


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