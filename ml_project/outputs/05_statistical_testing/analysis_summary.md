# Part 1E: Statistical Testing Deep Dive - Analysis Summary

## Date: October 19, 2025

## Overview
This analysis was designed to teach and apply core statistical tests to Premier League data. Due to data availability constraints, the analysis was adapted to work with the available datasets while still demonstrating all key statistical concepts.

## Data Availability

### Available Data:
- **Current Season Fixtures (2025-2026)**: 60 completed matches
  - Can be used for home advantage analysis
  - Limited to current season only

- **Historical Squad Data (2020-2025)**: 100 squad-season records at Gameweek 38
  - 5 complete seasons
  - Perfect for team-level comparisons

- **Historical Player Data (2020-2025)**: 2,588 player records at Gameweek 38
  - 5 complete seasons
  - Excellent for position-based analysis

### Data Limitation:
- Historical match-level fixtures (2020-2025) are not available in the database
- Only current season (2025-2026) has match-level data

## Adapted Analysis Plan

### Statistical Tests Performed:

#### 1. **Independent T-Test: Top 6 vs Bottom 14 Teams**
**Data**: Historical squad data (2020-2025, GW38)
**Sample Size**: 100 squad-seasons (30 Top 6, 70 Bottom 14)
**Metrics Compared**:
- Goals Scored
- Goals Against
- Total Shots
- Shots on Target

**Learning Objectives**:
- Hypothesis testing for two independent groups
- Calculating and interpreting Cohen's d effect size
- Understanding statistical vs practical significance

#### 2. **ANOVA: Goal Scoring by Player Position**
**Data**: Historical player data (2020-2025, GW38)
**Sample Size**: 2,588 players
**Analysis**:
- One-way ANOVA comparing FW, MF, DF, GK goal scoring
- Post-hoc pairwise comparisons with Bonferroni correction
- Effect size calculation for position differences

**Learning Objectives**:
- Comparing 3+ groups simultaneously
- Post-hoc testing procedures
- Multiple comparison corrections

#### 3. **Chi-Square Goodness of Fit (Optional): Current Season Home Advantage**
**Data**: Current season fixtures (2025-2026)
**Sample Size**: 60 completed matches
**Analysis**:
- Test if match outcomes deviate from equal distribution (33.3% each)
- Calculate chi-square statistic and p-value

**Note**: Limited to current season due to data availability

## Key Statistical Concepts Covered

### 1. Hypothesis Testing Framework
- Null hypothesis (H₀) vs Alternative hypothesis (H₁)
- P-values and significance levels (α = 0.05)
- Type I and Type II errors

### 2. Effect Sizes
- **Cohen's d**: For t-tests
  - Small: 0.2-0.5
  - Medium: 0.5-0.8
  - Large: ≥0.8
- **Cramér's V**: For chi-square tests
- **Why effect sizes matter**: Statistical significance ≠ practical significance

### 3. Multiple Comparison Problem
- Bonferroni correction for post-hoc tests
- Family-wise error rate control
- When and why to use corrections

### 4. Test Selection Criteria
- **Independent T-Test**: Two separate groups
- **Paired T-Test**: Same subjects, two conditions (not applicable with available data)
- **ANOVA**: 3+ groups
- **Chi-Square**: Categorical variables

## ML Feature Engineering Implications

### Team Tier Effects
- Top 6 teams show significantly different performance metrics
- **Feature Idea**: Create tier-based categorical features
- **Model Implication**: Consider separate models for different tiers

### Position Effects
- Player positions have dramatically different goal-scoring patterns
- **Feature Idea**: Position-specific baselines and expectations
- **Model Implication**: Weight position-adjusted metrics in player value models

### Statistical Rigor for ML
1. Always report both p-values AND effect sizes
2. Large sample sizes can make tiny differences "significant"
3. Focus on practical significance for feature engineering
4. Use statistical tests to validate feature importance

## Deliverables

### Notebook:
`ml_project/notebooks/01_exploratory_data_analysis/01e_statistical_testing.ipynb`

### Outputs:
`ml_project/outputs/05_statistical_testing/`
- `summary_report.txt`: Comprehensive findings
- `top6_vs_rest_summary_table.csv`: Independent t-test results
- `top6_vs_rest_boxplots.png`: Visual comparison
- `position_goals_boxplot.png`: ANOVA visualization
- `position_pairwise_comparisons.csv`: Post-hoc test results (if applicable)

## Recommendations for Future Analysis

### To Enable Full Home/Away Analysis:
1. Load historical match-level data into `analytics_fixtures` table
2. Ensure `is_completed` flag is set correctly
3. Include home_score, away_score, home_team, away_team columns

### Alternative Approaches:
1. Use squad-level home/away aggregates if available
2. Create synthetic match data from squad statistics
3. Focus on current season for match-level insights

## Learning Outcomes

By completing this analysis, you have learned:

✅ How to choose appropriate statistical tests for different data types
✅ How to calculate and interpret effect sizes
✅ When and how to apply multiple comparison corrections
✅ How to communicate statistical findings (p-values + effect sizes + practical significance)
✅ How statistical testing informs ML feature engineering

## Next Steps

1. **Part 1F**: Time series analysis and trend detection
2. **Part 2**: Feature engineering based on statistical insights
3. **Part 3**: Predictive modeling with statistically-validated features

---

## Notes

This analysis demonstrates core statistical testing principles using real Premier League data. While the original plan included paired t-tests for home vs away goal scoring, the adapted analysis maintains pedagogical value by focusing on tests that are applicable to the available data.

The statistical rigor and methodological approach remain unchanged, ensuring that all learning objectives are met.
