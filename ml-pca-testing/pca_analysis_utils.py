"""
PCA Analysis Utilities for Comprehensive Testing

Helper functions to keep notebooks clean and focused on analysis.
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from typing import Dict, List, Tuple
import json

# Load full category definitions
with open('full_categories.json', 'r') as f:
    FULL_CATEGORIES = json.load(f)


def prepare_category_data(df, category_name, negative_metrics):
    """
    Extract and prepare data for a category.

    Args:
        df: DataFrame with squad data
        category_name: Name of category
        negative_metrics: Set of metrics to invert

    Returns:
        DataFrame with prepared data, list of available metrics
    """
    metrics = FULL_CATEGORIES[category_name]['metrics']

    # Filter to available metrics
    available_metrics = [m for m in metrics if m in df.columns]

    # Create category dataframe
    base_cols = ['squad_name']
    if 'season' in df.columns:
        base_cols.append('season')

    cat_df = df[base_cols + available_metrics].copy()

    # Handle missing values - fill with 0
    for metric in available_metrics:
        if cat_df[metric].isna().any():
            cat_df[metric].fillna(0, inplace=True)

    # Invert negative metrics
    inverted_count = 0
    for metric in available_metrics:
        if metric in negative_metrics:
            cat_df[metric] = -cat_df[metric]
            inverted_count += 1

    return cat_df, available_metrics, inverted_count


def fit_pca_model(X, variance_threshold=0.80):
    """
    Fit PCA and return comprehensive results.

    Args:
        X: Data matrix (n_samples × n_features)
        variance_threshold: Cumulative variance threshold

    Returns:
        Dictionary with PCA results
    """
    # Standardize
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Fit PCA
    pca = PCA()
    X_pca = pca.fit_transform(X_scaled)

    # Determine components
    cumsum_var = np.cumsum(pca.explained_variance_ratio_)
    n_components = np.argmax(cumsum_var >= variance_threshold) + 1

    return {
        'pca': pca,
        'scaler': scaler,
        'X_pca': X_pca,
        'X_scaled': X_scaled,
        'n_components': n_components,
        'pc1_variance': pca.explained_variance_ratio_[0],
        'cumulative_variance': cumsum_var[n_components-1],
        'explained_variance_ratio': pca.explained_variance_ratio_
    }


def get_top_loadings(pca, metrics, n_top=10, component=0):
    """
    Get top N contributors to a principal component.

    Args:
        pca: Fitted PCA object
        metrics: List of metric names
        n_top: Number of top contributors
        component: Which component (0 = PC1, 1 = PC2, etc.)

    Returns:
        Series with top loadings
    """
    loadings = pd.Series(np.abs(pca.components_[component]), index=metrics)
    return loadings.nlargest(n_top)


def calculate_weighted_scores(X_pca, explained_variance_ratio, n_components):
    """
    Calculate weighted composite scores.

    Args:
        X_pca: Transformed data from PCA
        explained_variance_ratio: Explained variance for each component
        n_components: Number of components to use

    Returns:
        Array of weighted scores
    """
    weights = explained_variance_ratio[:n_components]
    weights_normalized = weights / weights.sum()
    scores = np.dot(X_pca[:, :n_components], weights_normalized)
    return scores, weights_normalized


def plot_loading_stability_heatmap(category_name, all_top_loadings, seasons):
    """
    Create heatmap showing which metrics appear in top 5 across years.

    Args:
        category_name: Name of category
        all_top_loadings: Dict of {season: [top metrics]}
        seasons: List of season names

    Returns:
        Figure object
    """
    # Get all unique metrics that appear in any top 5
    all_metrics = list(set([m for loadings in all_top_loadings.values() for m in loadings[:5]]))

    # Create matrix: metrics × seasons
    heatmap_data = []
    for metric in all_metrics:
        row = []
        for season in seasons:
            top_5 = all_top_loadings[season][:5]
            if metric in top_5:
                rank = top_5.index(metric) + 1
                row.append(6 - rank)  # Invert: 5=1st, 4=2nd, etc.
            else:
                row.append(0)
        heatmap_data.append(row)

    df_heatmap = pd.DataFrame(heatmap_data, index=all_metrics, columns=seasons)

    # Plot
    fig, ax = plt.subplots(figsize=(12, max(6, len(all_metrics) * 0.4)))
    sns.heatmap(df_heatmap, annot=True, fmt='.0f', cmap='YlOrRd',
                cbar_kws={'label': 'Rank (5=1st, 4=2nd, ...)'}, ax=ax)
    ax.set_title(f'{category_name}: Top 5 PC1 Loadings Stability', fontsize=14, fontweight='bold')
    ax.set_xlabel('Season', fontsize=12)
    ax.set_ylabel('Metric', fontsize=12)
    plt.tight_layout()

    return fig


def plot_pc1_variance_comparison(results_dict):
    """
    Bar chart comparing PC1 variance across categories.

    Args:
        results_dict: Dict of {category_name: results_dict}

    Returns:
        Figure object
    """
    categories = list(results_dict.keys())
    pc1_variances = [results_dict[cat]['pc1_variance'] for cat in categories]

    fig, ax = plt.subplots(figsize=(12, 6))
    bars = ax.bar(range(len(categories)), pc1_variances, color='steelblue', alpha=0.7)
    ax.set_xticks(range(len(categories)))
    ax.set_xticklabels(categories, rotation=45, ha='right')
    ax.set_ylabel('PC1 Explained Variance', fontsize=12)
    ax.set_title('PC1 Variance Across Categories', fontsize=14, fontweight='bold')
    ax.axhline(y=0.5, color='r', linestyle='--', alpha=0.5, label='50% threshold')
    ax.set_ylim([0, max(pc1_variances) * 1.1])
    ax.legend()
    ax.grid(axis='y', alpha=0.3)

    # Add value labels on bars
    for i, (bar, val) in enumerate(zip(bars, pc1_variances)):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.01,
                f'{val:.1%}', ha='center', va='bottom', fontsize=10, fontweight='bold')

    plt.tight_layout()
    return fig


def create_summary_table(results_dict):
    """
    Create summary table of PCA results across categories.

    Args:
        results_dict: Dict of {category_name: results_dict}

    Returns:
        DataFrame with summary
    """
    rows = []
    for category_name, results in results_dict.items():
        rows.append({
            'Category': category_name,
            'PC1 Variance': f"{results['pc1_variance']:.1%}",
            'Components Used': results['n_components'],
            'Total Variance': f"{results['cumulative_variance']:.1%}",
            'Top Loading': f"{results.get('top_metric', 'N/A')} ({results.get('top_loading', 0):.4f})"
        })

    return pd.DataFrame(rows)


def analyze_stability(all_top_loadings, seasons, n_top=5):
    """
    Analyze loading stability across seasons.

    Args:
        all_top_loadings: Dict of {season: [metrics in order]}
        seasons: List of season names
        n_top: Number of top metrics to consider

    Returns:
        Dict with stability metrics
    """
    # Count appearances in top N
    metric_counts = {}
    for season in seasons:
        for metric in all_top_loadings[season][:n_top]:
            metric_counts[metric] = metric_counts.get(metric, 0) + 1

    # Calculate stability
    stable_metrics = [m for m, count in metric_counts.items() if count >= len(seasons) * 0.8]
    stability_score = len(stable_metrics) / n_top if n_top > 0 else 0

    return {
        'metric_frequencies': sorted(metric_counts.items(), key=lambda x: x[1], reverse=True),
        'stable_metrics': stable_metrics,
        'stability_score': stability_score,
        'is_stable': stability_score >= 0.6  # 60% of top N appear in 80%+ of seasons
    }


def print_analysis_header(title, width=80):
    """Print formatted section header."""
    print(f"\n{'='*width}")
    print(f"{title.center(width)}")
    print(f"{'='*width}\n")


def print_category_results(category_name, results, metrics):
    """Print formatted results for a category."""
    print(f"\n🎯 {category_name}")
    print(f"   {'─'*70}")
    print(f"   PC1 Variance: {results['pc1_variance']:.1%}")
    print(f"   Components:   {results['n_components']} (explaining {results['cumulative_variance']:.1%})")
    print(f"   Metrics:      {len(metrics)}")

    # Top loadings
    top_loadings = get_top_loadings(results['pca'], metrics, n_top=5)
    print(f"\n   Top 5 PC1 Contributors:")
    for i, (metric, loading) in enumerate(top_loadings.items(), 1):
        print(f"      {i}. {metric:40s} {loading:.4f}")
