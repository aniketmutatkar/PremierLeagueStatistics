# PCA Testing Branch: Exploration & Findings

## Overview

This branch contains exploratory analysis testing Principal Component Analysis (PCA) for Premier League squad performance metrics. The goal was to determine whether PCA-based weighting provides advantages over equal-weighting or other methods for creating composite scores and predicting outcomes.

**Key Finding:** PCA is excellent for certain use cases (finding patterns, team archetypes, dimensionality reduction) but may not be optimal for predictive modeling of specific outcomes like goals or league position. For prediction tasks, correlation-based or Lasso regression weights perform better.

---

## Tests Conducted

### 1. **10-Year Baseline PCA (2015-2024)**
**Notebook:** `02_10year_baseline_pca_2015-2025.ipynb`

**Objective:** Test if a 9-year baseline (2015-2024) produces stable PCA loadings that can be applied to the completed 2024-2025 season.

**Methodology:**
- Trained PCA on 9 seasons (2015-2024, 180 squad-seasons)
- Applied learned weights to 2024-2025 season
- Tested stability: checked if same metrics appear in top 5 loadings across 80%+ of years

**Results:**
```
STABLE CATEGORIES (use baseline approach):
- attacking_output: 80% stability
- creativity: 60% stability
- ball_progression: 80% stability

UNSTABLE CATEGORIES (use dynamic approach):
- passing: 40% stability
- defending: 40% stability
- physical_duels: 0% stability (no metrics consistent)
- possession: 40% stability
```

**PC1 Variance Explained:**
```
attacking_output: 55.3%
creativity: 73.3%
passing: 87.5%
ball_progression: 84.9%
defending: 46.1%
physical_duels: 55.6%
possession: 77.6%
```

**Conclusion:** Some categories (attacking, ball progression) have stable patterns over time. Others (defending, physical duels) require season-specific fitting.

---

### 2. **Individual Season Comparison (2020-2025)**
**Notebook:** `03_individual_season_pca_comparison_2020-2025.ipynb`

**Objective:** Test PCA stability across recent 5 seasons and validate whether PCA scores correlate with actual league performance.

**Methodology:**
- Fitted 35 PCA models (7 categories × 5 seasons)
- Tested stability: which metrics appear in top 5 across seasons
- Correlated PCA composite scores with league position
- Compared PCA weights vs equal-weight composite scores

**Key Results:**

**Stability (5 seasons):**
```
HIGHLY STABLE:
- ball_progression: 100%
- attacking_output: 80%
- creativity: 80%
- passing: 80%
- possession: 80%

UNSTABLE:
- defending: 40%
- physical_duels: 40%
```

**Correlation with League Position:**
```
STRONG PREDICTORS:
- attacking_output: +0.871 (very strong)
- creativity: +0.844 (very strong)
- ball_progression: +0.732 (strong)
- passing: +0.712 (strong)
- possession: +0.692 (moderate)

WEAK PREDICTORS:
- physical_duels: +0.457 (weak)
- defending: -0.111 (essentially useless, sometimes negative!)
```

**PCA vs Equal-Weight Performance:**
```
PCA SIGNIFICANTLY BETTER:
- possession: +0.235 advantage

PCA SLIGHTLY BETTER:
- passing: +0.049 advantage

NO DIFFERENCE:
- attacking_output: -0.002
- creativity: -0.002
- ball_progression: -0.018

EQUAL-WEIGHT BETTER:
- defending: -0.205 (but both perform poorly)
- physical_duels: -0.056
```

**Conclusion:**
- PCA provides minor improvements for possession and passing
- For attacking/creativity/ball_progression, simple equal-weighting works just as well
- Defending metrics don't predict league position well (regardless of weighting)

---

### 3. **Process Metrics → Goal Correlation**
**Notebook:** `04_process_metrics_goal_correlation.ipynb`

**Objective:** Strip out tautological metrics (goals, assists, GCA) and identify which PROCESS metrics predict goal scoring.

**Methodology:**
- Defined 7 process categories (70 metrics total)
- Excluded: goals, assists, goal-creating actions (outcomes)
- Included: shots, key passes, carries, crosses (processes)
- Fitted PCA on process metrics
- Correlated process scores with actual goals scored

**Process Category Results:**
```
STRONG PREDICTORS OF GOALS:
- shot_generation: ρ=+0.883 (very strong, stable)
- chance_creation: ρ=+0.772 (strong)
- possession_quality: ρ=+0.709 (strong)

MODERATE PREDICTORS:
- build_up_passing: ρ=+0.664
- ball_progression: ρ=+0.658

WEAK/NEGATIVE PREDICTORS:
- set_pieces: ρ=+0.386 (inconsistent)
- defensive_actions: ρ=-0.213 (negative!)
```

**Individual Metric Importance (shot_generation):**
```
Top predictors of goals:
1. shots_on_target: +0.851
2. shots_on_target_per_90: +0.851
3. shots: +0.739
4. shots_per_90: +0.739
5. shot_accuracy: +0.576
```

**Conclusion:** Shot generation (volume + quality) is by far the strongest process predictor of goals. Defensive actions negatively correlate (teams that defend more score less).

---

### 4. **Weighting Methods Comparison**
**Analysis:** Compared PCA weights vs Correlation weights vs Lasso weights for predicting goals

**Methodology:**
- Tested on shot_generation category (9 metrics)
- Created 3 different composite scores using different weighting:
  1. **PCA Weights:** Based on explained variance
  2. **Correlation Weights:** Based on correlation with goals
  3. **Lasso Weights:** Regression coefficients (auto feature selection)
- Measured: which composite score best predicts goals

**Results (Average across 5 seasons):**
```
PCA Weights:         ρ = 0.883
Correlation Weights: ρ = 0.890 (+0.8% improvement)
Lasso Weights:       ρ = 0.903 (+2.3% improvement) ✓ WINNER
```

**Key Insight - Lasso Feature Selection:**
```
Example (2020-2021):
- Correlation used all 9 metrics → ρ = +0.930
- Lasso kept only 6/9 metrics → ρ = +0.957 (better!)

Lasso automatically removed redundant metrics:
- Kept: shots_per_90, shot_accuracy (unique info)
- Zeroed: shots, shots_on_target (redundant)
```

**Conclusion:** For prediction tasks, Lasso/Correlation weights outperform PCA weights. PCA optimizes for variance, not prediction.

---

## Key Findings Summary

### What PCA Is Good At:

1. **Finding Team Archetypes (Unsupervised Learning)**
   - Discovering playing styles without labels
   - Example: High PC1 = possession teams, Low PC1 = counter-attacking teams
   - PCA found: Man City/Arsenal (high volume) vs Nottingham Forest/Brentford (low volume)

2. **Dimensionality Reduction / Data Compression**
   - Reducing 169 metrics → 10-12 principal components
   - Capturing 80-90% of variance with far fewer features
   - Useful for visualization (plot teams in 2D instead of 169D)

3. **Creating "General Quality" Scores**
   - Unsupervised overall ratings (like FIFA scores)
   - Global PC1 captured 30.75% of all variance across 169 metrics
   - Teams high in PC1 are generally strong across the board

4. **Understanding Metric Redundancy**
   - Identifying correlated metrics (e.g., passes_completed + touches)
   - Shows which metrics measure the same underlying construct
   - Possession category: 11 metrics → 3 components (93.8% variance)

5. **Exploring Data Patterns**
   - Finding which metrics vary together naturally
   - Discovering the dimensionality of performance (attacking = 1D, defending = 8D)
   - Understanding category complexity

### What PCA Is NOT Good At:

1. **Predicting Specific Outcomes**
   - PCA maximizes variance, not prediction accuracy
   - For predicting goals/wins: Lasso/Correlation weights are +2.3% better
   - Example: offsides has high variance → high PCA weight, but low correlation with goals

2. **Supervised Learning Tasks**
   - PCA doesn't use the target variable (goals, league position)
   - Correlation/Lasso methods directly optimize for the outcome
   - When you have a specific objective, use supervised methods

3. **Feature Selection for Prediction**
   - PCA keeps all features (just re-weights them)
   - Lasso zeros out redundant features automatically
   - For cleaner models: Lasso > PCA

### When to Use Each Method:

| Method | Use Case | Example |
|--------|----------|---------|
| **PCA Weights** | Unsupervised: finding patterns, team styles, general ratings | "What team archetypes exist in the league?" |
| **Correlation Weights** | Supervised: quick prediction, feature ranking | "Which metrics predict goals?" |
| **Lasso Weights** | Supervised: production models, handling multicollinearity | "What are the KEY unique drivers of success?" |
| **Equal Weights** | Simple baseline when metrics are uncorrelated | "All metrics are equally important" (rare) |

---

## Specific Insights from Testing

### Insight 1: Attacking Metrics Are Highly Redundant
- PC1 explains 52-55% of variance (very high)
- Most attacking metrics measure the same thing: "goal threat"
- Equal-weighting performs nearly as well as PCA (difference: -0.002 correlation)
- Conclusion: For attacking, simple averaging is sufficient

### Insight 2: Defending Is Multifaceted
- PC1 explains only 46% of variance
- Needs 8 components to capture 80% of variance
- Tackling ≠ Blocking ≠ Goalkeeping ≠ Positioning
- Defending scores don't predict league position well (-0.111 correlation!)
- Conclusion: Defense is complex and doesn't directly correlate with winning

### Insight 3: Shot Generation >>> Defensive Actions
- Shot generation: +0.883 correlation with goals
- Defensive actions: -0.213 correlation with goals
- Teams that defend more tend to score less (possession trade-off)
- Conclusion: Focus on offensive processes for goal prediction

### Insight 4: Per-90 Metrics Dominate PC1
- Across all categories, 80% of top PC1 loadings are per-90 metrics
- Normalization matters: accounts for playing time
- Raw counts have lower loadings
- Conclusion: Always use per-90 normalized metrics when possible

### Insight 5: Possession/Passing Have Stable Patterns
- High PC1 variance (87-94%)
- Stable across seasons (80%+ stability)
- PCA provides small advantage (+0.049 to +0.235)
- Conclusion: These categories benefit most from PCA

---

## Conclusions & Recommendations

### For Predictive Modeling (Goals, League Position, Wins):
✅ **USE:** Correlation-based or Lasso weights
- Directly optimize for the outcome
- Handle multicollinearity better
- Provide feature selection (Lasso)
- +2.3% better performance than PCA

❌ **DON'T USE:** PCA weights
- Optimizes variance, not prediction
- Can overweight irrelevant high-variance metrics
- No automatic feature selection

### For Exploratory Analysis (Patterns, Archetypes):
✅ **USE:** PCA
- Finds natural groupings and styles
- Identifies redundant metrics
- Reduces dimensionality for visualization
- No labels required (unsupervised)

### For Simple Composite Scores:
✅ **USE:** Equal-weighting (baseline) or Correlation-weighting
- Equal-weighting performs well for attacking/creativity/ball_progression
- Correlation-weighting is simple and interpretable
- PCA adds complexity with minimal benefit for simple categories

### For Production Systems:
✅ **RECOMMENDED APPROACH:**
1. Use Lasso regression for feature selection
2. Use correlation weights for composite scores
3. Reserve PCA for exploratory analysis and visualization
4. Use equal-weighting as a simple baseline

---

## Files in This Branch

### Notebooks:
- `02_10year_baseline_pca_2015-2025.ipynb` - Long-term stability testing
- `03_individual_season_pca_comparison_2020-2025.ipynb` - Recent season comparison
- `04_process_metrics_goal_correlation.ipynb` - Process metrics analysis + weighting comparison

### Utilities:
- `pca_analysis_utils.py` - Helper functions for PCA fitting, stability analysis
- `full_categories.json` - Complete category definitions (169 metrics)

### Database:
- `../data/premierleague_analytics.duckdb` - Analytics database with squad metrics

---

## What This Branch Taught Us

1. **PCA is not a silver bullet** - It's a specific tool for specific problems (unsupervised pattern finding)

2. **For prediction, use supervised methods** - Correlation/Lasso directly optimize for outcomes

3. **Equal-weighting is often "good enough"** - When metrics are well-chosen and correlated, simple averaging works

4. **Defense doesn't predict winning** - At least not with squad-level metrics. Defending correlates poorly with league position

5. **Shot generation is king** - Most predictive process for goal scoring

6. **Context matters for method selection** - No single "best" approach for all scenarios

---

## Next Steps

### If Continuing PCA Exploration:

**Potential Use Cases:**
1. **Position-Specific PCA** - Different patterns for top 6 vs bottom 14 teams
2. **Player-Level PCA** - Find player archetypes (playmakers, box-to-box, etc.)
3. **Opponent-Adjusted PCA** - Account for strength of opposition
4. **Time-Series PCA** - How team styles evolve over a season
5. **Combined Approach** - PCA for exploration → Lasso for prediction

**Areas to Explore:**
- Kernel PCA for non-linear relationships
- Sparse PCA for more interpretable components
- Dynamic PCA that updates as season progresses
- Cross-category interactions (attacking + defending synergies)

### If Moving to Production:

**Recommended Implementation:**
1. Use **Correlation weights** for quick composite scores
2. Use **Lasso regression** for production prediction models
3. Use **PCA** for exploratory dashboards and team comparisons
4. Keep **equal-weighting** as a simple baseline for validation

---

## Lessons Learned

### Technical:
- Always invert negative metrics before PCA
- Standardization is critical (mean=0, std=1)
- Use zero-fill for missing values (not median)
- 80% variance threshold is a good balance
- Per-90 metrics are generally better than raw counts

### Methodological:
- Match method to objective (unsupervised vs supervised)
- Don't assume PCA is better than simple methods
- Test multiple approaches and compare
- Use actual outcomes (goals, wins) to validate

### Practical:
- Simple methods are often sufficient
- Complexity should provide clear benefits
- Interpretability matters for stakeholders
- Document assumptions and limitations

---

**Branch Status:** Exploration Complete
**Merge Recommendation:** Keep as reference branch, use findings to inform main branch implementation
**Date:** 2025-10-28
**Authors:** Aniket + Claude
