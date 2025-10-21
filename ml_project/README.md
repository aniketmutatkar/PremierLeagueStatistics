# Premier League ML Project

Machine learning experimentation and model development for Premier League match prediction.

## Structure
- `notebooks/`: Jupyter notebooks for exploration and analysis
- `outputs/`: Generated charts, reports, and analysis results
- `data/`: ML-specific data artifacts (processed datasets, features, splits)
- `models/`: Saved trained models and scalers
- `scripts/`: Reusable ML utility functions
- `docs/`: Project documentation and roadmaps

## Project Status

**Current Phase:** Lesson 3 (Model Training & Evaluation)
**Last Updated:** October 21, 2025

### Completed Work

#### Lesson 1: Exploratory Data Analysis (EDA) ✅
- [x] **Part 1A: Data Inspection** - Database schema, data quality, completeness analysis
- [x] **Part 1B: Goals Analysis** - Historical goal patterns, position tiers, current season projections
- [x] **Part 1C: Composite Scores Analysis** - 7 squad categories, tier-specific performance patterns
- [x] **Part 1D: Root Cause Analysis** - 49 gold standard features, individual stat patterns
- [x] **Part 1E: Statistical Testing** - Chi-Square, T-Tests, ANOVA validation of key hypotheses

#### Lesson 2: Feature Engineering ✅
- [x] **Part 2A: Historical Features** - Transform squad data into match-level features (~160 features per match)
  - 6,080 historical matches (2020-2025 seasons) converted to ML-ready format
  - Feature catalog with 160+ engineered features
  - Data saved: `match_features_historical.csv` (1.3MB)

#### Lesson 3: Model Training & Evaluation (In Progress)
- [x] **Part 3A: Baseline Model** - Random Forest with tier feature analysis
  - Match outcome prediction accuracy: ~58% on test set
  - Educational deep-dive on confusion matrices, precision/recall
  - Tier feature hypothesis testing (no significant improvement found)
- [x] **Part 3B: Goal Prediction Model** - Predicting actual goals scored
  - Separate models for home_goals and away_goals
  - MAE: ~1.2 goals, RMSE: ~1.5 goals
  - Explored regression vs classification approaches
- [ ] Part 3C: Model optimization and hyperparameter tuning
- [ ] Part 3D: Production deployment preparation

## Data Assets

### Processed Datasets
Located in `ml_project/data/`:
- `match_features_historical.csv` - 6,080 matches with ~160 features each (1.3MB)
- `feature_catalog_historical.csv` - Feature definitions and metadata
- `premierleague_analytics.duckdb` - Analytical database with squad/fixture/player data

### Trained Models
Located in `ml_project/models/`:
- `rf_no_tiers.pkl` - Random Forest baseline (match outcome prediction)
- `rf_with_tiers.pkl` - Random Forest with tier interaction features
- `rf_home_goals.pkl` - Random Forest regressor for home team goals
- `rf_away_goals.pkl` - Random Forest regressor for away team goals
- `feature_scaler.pkl` - StandardScaler for feature normalization

## Key Findings Summary

### Part 1A: Data Inspection (Oct 15, 2025)
**Location:** `outputs/01_data_inspection/`
- Analyzed 15 complete seasons (2010-2025) + current season (2025-2026)
- Verified 300 historical squad records at GW38
- Identified data quality issues (gameweek variations)
- Generated schema documentation and sample data

### Part 1B: Goals Analysis (Oct 15, 2025)
**Location:** `outputs/02_goals_analysis/`

**Key Findings:**
- **Average full season:** 51.3 goals scored, 51.3 goals conceded
- **Top 4 threshold:** 71+ goals scored, ≤36 goals conceded
- **Relegation threshold:** <54 goals scored
- **Historical records:**
  - Most goals: Man City 2017-18 (103 goals, 100 pts)
  - Best defense: Liverpool 2018-19 (22 conceded, 97 pts)
  - Worst attack: Sheffield Utd 2020-21 (19 goals)
  - Worst defense: Sheffield Utd 2023-24 (104 conceded)

### Part 1C: Composite Scores Analysis (Oct 19, 2025)
**Location:** `outputs/03_composite_scores_analysis/`

**Key Findings:**
- Identified 7 squad performance categories: overall, defensive, offensive, possession, shooting, passing, discipline
- Top 4 teams excel across all metrics (composite scores 15-25 points higher)
- Clear separation between position tiers validates league structure
- Balance matters: elite teams strong in both attack AND defense

### Part 1D: Root Cause Analysis (Oct 19, 2025)
**Location:** `outputs/04_individual_stats/`

**Key Findings:**
- Identified 49 gold standard features for ML modeling
- Position predicts goals: FW score 7x more than DF (5.09 vs 0.73 goals/season)
- Elite teams show different success patterns:
  - Top 4: High possession, high pass completion
  - Relegation: Low shot volume, poor defensive stats
- Tier-specific strategies confirmed

### Part 1E: Statistical Testing (Oct 20, 2025)
**Location:** `outputs/05_statistical_testing/`

**Key Validated Hypotheses:**
1. **Home Advantage Exists:** +9.2% win rate (43.2% home vs 34% away, p<0.001)
2. **Teams Score More at Home:** +3.98 goals/season (Cohen's d = 0.576, large effect)
3. **Elite Teams Differ Massively:** Top 6 vs Bottom 14 (Cohen's d > 1.3, huge effect)
4. **Position Predicts Goals:** FW vs DF highly significant (p<0.001)
5. **Tier Separation:** Top 4 vs Relegation zone statistically distinct across all metrics

### Part 2A: Historical Features (Oct 20, 2025)
**Location:** `outputs/06_feature_engineering/`

**Accomplishments:**
- Transformed 300 squad-season records (GW38, 2020-2025) into 6,080 match-level observations
- Created ~160 engineered features per match:
  - Squad aggregates (49 gold standard stats × 2 teams)
  - Derived features (ratios, efficiency metrics, home advantage indicators)
  - Historical context (season, gameweek, tier information)
- Fixed data quality issues (column naming, duplicate match_ids)
- Generated feature catalog for documentation

### Part 3A: Baseline Model Training (Oct 20, 2025)
**Location:** `outputs/07_model_training/`

**Results:**
- **Random Forest Classifier** (match outcome: Home Win / Draw / Away Win)
  - Test accuracy: ~58% (baseline model)
  - Best at predicting home wins (precision: 61%, recall: 73%)
  - Struggles with draws (precision: 35%, recall: 20%)
- **Tier Feature Hypothesis:** Tested but no significant improvement
  - Model with tier interactions: 57.7% accuracy
  - Model without tiers: 58.2% accuracy
  - Conclusion: Tier information already captured in raw stats
- **Feature Importance:** Top predictors are squad quality metrics (goals, shots, composites)

### Part 3B: Goal Prediction Model (Oct 20, 2025)
**Location:** `outputs/08_goal_prediction/`

**Results:**
- **Regression Approach:** Random Forest Regressors
  - Home Goals MAE: 1.19, RMSE: 1.47
  - Away Goals MAE: 1.21, RMSE: 1.51
  - Can predict exact goal totals within ~1.2 goals on average
- **Insights:**
  - Goal prediction is inherently noisy (high variance in actual goals)
  - Models capture general trends but struggle with outliers (0-0 draws, 5+ goal games)
  - Squad quality strongly predicts goals scored

## Notebooks

All notebooks are educational and include extensive markdown explanations, visualizations, and statistical analysis.

### Lesson 1: Exploratory Data Analysis
- `01a_data_inspection.ipynb` - Database exploration, schema analysis, data quality checks
- `01b_goals_analysis.ipynb` - Goal patterns by tier, historical benchmarks, current season tracking
- `01c_composite_scores_analysis.ipynb` - 7 squad performance categories, tier separation
- `01d_root_cause_goals_analysis.ipynb` - 49 individual features, position analysis, tier strategies
- `01e_statistical_testing.ipynb` - Hypothesis validation, Chi-Square, T-Tests, ANOVA, Cohen's d

### Lesson 2: Feature Engineering
- `02a_historical_features.ipynb` - Transform squad data → match features (6,080 matches, ~160 features)

### Lesson 3: Model Training
- `03a_baseline_model.ipynb` - Random Forest baseline, tier hypothesis testing, evaluation metrics deep-dive
- `03b_goal_prediction_model.ipynb` - Regression models for predicting actual goals (home/away)

## Next Steps

### Immediate (Lesson 3 Continuation)
1. Model optimization and hyperparameter tuning
2. Cross-validation and performance stability testing
3. Feature selection and dimensionality reduction
4. Production deployment preparation

### Future Enhancements (Lesson 4+)
See `docs/future_features.md` for detailed roadmap including:
- **Phase 2:** Lineup-based predictions (manual lineup entry, key player indicators)
- **Phase 3:** Auto-populated lineups (injury tracking, lineup prediction model)
- **Phase 4:** Player-vs-player matchups (positional battle modeling)
- **Advanced Features:** Live match updates, form-adjusted predictions, confidence intervals

**Target Accuracy Progression:**
- Phase 1 (Current): 55-60% ✅
- Phase 2 (Lineups): 58-63%
- Phase 3 (Auto-lineups): 60-65%
- Phase 4 (Matchups): 62-68%

## Documentation

- `docs/future_features.md` - Comprehensive roadmap of planned enhancements and optimizations
- `outputs/02_goals_analysis/README.md` - Detailed findings from goals analysis
- Each output directory contains generated visualizations and summary reports

## How to Use

### Running Notebooks
```bash
cd ml_project/notebooks
jupyter notebook
```

### Loading Trained Models
```python
import pickle
import pandas as pd

# Load model
with open('../models/rf_no_tiers.pkl', 'rb') as f:
    model = pickle.load(f)

# Load scaler
with open('../models/feature_scaler.pkl', 'rb') as f:
    scaler = pickle.load(f)

# Load features
df = pd.read_csv('../data/match_features_historical.csv')

# Make predictions
predictions = model.predict(df)
```

### Making Match Predictions
```python
# Example: Predict Arsenal vs Chelsea
home_features = get_squad_features('Arsenal', season='2025-2026')
away_features = get_squad_features('Chelsea', season='2025-2026')
match_features = combine_features(home_features, away_features, is_home=True)

prediction = model.predict_proba([match_features])
# Returns: [prob_away_win, prob_draw, prob_home_win]
```

## Project Timeline

- **Oct 15, 2025:** Lesson 1A-1B (Data inspection, goals analysis)
- **Oct 19, 2025:** Lesson 1C-1D (Composite scores, root cause analysis)
- **Oct 20, 2025:** Lesson 1E (Statistical testing), Lesson 2A (Feature engineering), Lesson 3A-3B (Model training)
- **Oct 21, 2025:** Documentation update, preparing for Lesson 3 continuation

## Key Learnings

1. **Tier separation is real:** Top 4 vs Relegation teams differ by 30+ goals/season and show statistically huge effects (Cohen's d > 1.3)
2. **Home advantage is significant:** +9.2% win rate, +4 goals/season at home
3. **Position matters for goals:** Forwards score 7x more than defenders
4. **Elite teams excel everywhere:** No single magic metric - consistent strength across all categories
5. **ML baseline achieved:** 58% match outcome accuracy with squad-level features only
6. **Tier features redundant:** Raw stats already capture tier information - explicit tier indicators don't help
7. **Goal prediction is noisy:** Best MAE is ~1.2 goals - inherent randomness in football scoring

---

**Last Updated:** October 21, 2025
**Project Status:** Active Development (Lesson 3)
**Next Milestone:** Model optimization and hyperparameter tuning
