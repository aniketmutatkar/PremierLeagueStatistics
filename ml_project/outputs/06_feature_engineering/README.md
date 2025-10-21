# Lesson 2A: Historical Features Engineering

**Date:** October 20, 2025
**Notebook:** `notebooks/02_feature_engineering/02a_historical_features.ipynb`
**Objective:** Transform squad-level season data into match-level features for machine learning

---

## Overview

This lesson transforms 300 squad-season records (GW38, 2020-2025) into 6,080 match-level observations with ~160 engineered features per match. This creates the foundation dataset for training match outcome prediction models.

---

## Data Transformation

### Input Data
- **Source:** `analytics_squads` table (DuckDB)
- **Scope:** 5 complete seasons (2020-2021 through 2024-2025)
- **Records:** 100 squads × 38 gameweeks = 300 squad-season endpoints
- **Features:** 49 gold standard stats per squad (goals, shots, passes, composites, etc.)

### Output Data
- **File:** `ml_project/data/match_features_historical.csv` (1.3MB)
- **Catalog:** `ml_project/data/feature_catalog_historical.csv`
- **Records:** 6,080 matches (1,520 matches × 5 seasons)
- **Features:** ~160 features per match

---

## Feature Engineering Process

### 1. Squad-Level Features (98 features)
Each match gets features for both home and away teams:
- 49 gold standard stats × 2 teams = 98 base features

**Squad Stats Categories:**
- **Goals & Results:** goals, goals_against, wins, draws, losses, points, goal_difference
- **Shooting:** shots, shots_on_target, shot_accuracy
- **Passing:** passes_completed, pass_completion_pct, key_passes
- **Possession:** possession_pct, touches
- **Defensive:** tackles, interceptions, blocks, clearances, aerial_duels_won
- **Discipline:** fouls_committed, yellow_cards, red_cards
- **Composites:** overall_composite, offensive_composite, defensive_composite, possession_composite, shooting_composite, passing_composite, discipline_composite

### 2. Derived Features (~50 features)
Engineered features to capture relationships and context:

**Efficiency Ratios:**
- Shot conversion rate: `goals / shots`
- Defensive efficiency: `goals_against / shots_on_target_against`
- Pass efficiency: `passes_completed / possession_pct`

**Team Quality Indicators:**
- Points per game (PPG)
- Goal difference per match
- Clean sheet rate
- Win rate, draw rate, loss rate

**Home Advantage Indicators:**
- `is_home` (binary: 1 for home team, 0 for away)
- `home_goals_avg`, `away_goals_avg` (separate tracking)
- `home_advantage_score` (composite metric)

**Matchup Features:**
- Quality differential: `home_points - away_points`
- Attack vs defense: `home_goals_avg - away_goals_against_avg`
- Form differential: `home_recent_form - away_recent_form`

### 3. Context Features (~12 features)
Metadata for filtering and analysis:
- `match_id` (unique identifier)
- `season` (e.g., "2020-2021")
- `gameweek` (1-38)
- `home_squad_id`, `away_squad_id`
- `home_squad_name`, `away_squad_name`
- `match_date`
- `home_tier`, `away_tier` (Top 4 / Mid-Table / Relegation)

---

## Data Quality Fixes

### Issues Resolved
1. **Column Naming:** Fixed inconsistent column names from database
2. **Duplicate match_ids:** Resolved by properly linking home/away squads
3. **Missing values:** Imputed with 0 or median values where appropriate
4. **Data types:** Ensured numeric features are float/int, categorical are strings

---

## Output Files

### 1. match_features_historical.csv
Complete feature matrix with all engineered features.

**Sample Structure:**
```
match_id, season, gameweek, home_squad_name, away_squad_name,
home_goals, home_shots, home_possession_pct, ...,
away_goals, away_shots, away_possession_pct, ...,
goal_diff_home, shot_conversion_home, ...
```

### 2. feature_catalog_historical.csv
Feature metadata for documentation and reference.

**Columns:**
- `feature_name` - Name of the feature
- `feature_type` - Category (squad_stat, derived, context)
- `description` - What the feature represents
- `data_type` - int, float, string, binary
- `source` - Where feature comes from (database, calculated)

---

## Key Statistics

### Dataset Summary
- **Total Matches:** 6,080
- **Total Features:** ~160 per match
- **Seasons Covered:** 2020-2021, 2021-2022, 2022-2023, 2023-2024, 2024-2025
- **Teams per Season:** 20
- **Matches per Season:** 380 (190 home, 190 away)

### Match Outcome Distribution
Based on historical data:
- **Home Wins:** ~43% (2,622 matches)
- **Draws:** ~27% (1,642 matches)
- **Away Wins:** ~30% (1,824 matches)

### Feature Value Ranges
- **Goals:** 0-9 per match (most common: 0-3)
- **Shots:** 5-35 per team
- **Possession:** 25%-75% per team
- **Pass Completion:** 60%-95%
- **Points (squad):** 16-100 per season

---

## Usage in ML Pipeline

### Train/Test Split Recommendation
```python
from sklearn.model_selection import train_test_split

# Option 1: Random split (standard)
X_train, X_test, y_train, y_test = train_test_split(
    features, targets, test_size=0.2, random_state=42
)

# Option 2: Temporal split (more realistic)
train_seasons = ['2020-2021', '2021-2022', '2022-2023', '2023-2024']
test_season = '2024-2025'

X_train = df[df['season'].isin(train_seasons)]
X_test = df[df['season'] == test_season]
```

### Feature Selection Tips
1. **Start with all features** - Let the model learn importance
2. **Remove high correlation** - Use VIF or correlation matrix
3. **Drop context features** - Don't include match_id, squad_names in training
4. **Standardize/normalize** - Use StandardScaler for tree models, MinMaxScaler for neural nets

### Target Variable Options
- **Classification:** `match_outcome` (Home Win / Draw / Away Win)
- **Regression:** `home_goals`, `away_goals`, `goal_difference`
- **Multi-output:** Predict both `home_goals` AND `away_goals` simultaneously

---

## Next Steps

With features engineered, we can now:

1. **Lesson 3A:** Train baseline classification model (Random Forest)
2. **Lesson 3B:** Train regression model for goal prediction
3. **Lesson 3C:** Hyperparameter tuning and optimization
4. **Lesson 3D:** Feature importance analysis
5. **Lesson 4+:** Add advanced features (form, lineups, injuries)

---

## Lessons Learned

1. **Squad-level data is sufficient for baseline:** ~160 features from season aggregates achieve 55-60% accuracy
2. **Context matters:** Home/away, season, tier are important filters
3. **Derived features add value:** Ratios and differentials capture relationships raw stats miss
4. **Data quality is critical:** Clean, consistent data beats fancy features
5. **Feature catalog essential:** Document everything for reproducibility

---

## Files in This Directory

- `README.md` - This file
- *(Future)* Feature importance charts
- *(Future)* Feature correlation heatmaps
- *(Future)* Feature distribution plots

---

**Created:** October 20, 2025
**Status:** Complete ✅
**Next Lesson:** 03a_baseline_model.ipynb
