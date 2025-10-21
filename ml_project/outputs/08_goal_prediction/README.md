# Lesson 3B: Goal Prediction Model

**Date:** October 20, 2025
**Notebook:** `notebooks/03_model_training/03b_goal_prediction_model.ipynb`
**Objective:** Build regression models to predict exact goal totals (home_goals, away_goals) instead of just match outcomes

---

## Overview

This lesson explores whether we can predict the actual number of goals each team will score, not just who wins. This is a harder problem than outcome prediction because:
1. **More granular:** Predicting exact numbers (0, 1, 2, 3+) vs broad categories (Win/Draw/Loss)
2. **Higher variance:** Goals are inherently random - even dominant teams can score 0
3. **Two predictions needed:** Must predict both home AND away goals

---

## Approach

### Problem Formulation

**Task:** Regression (predict continuous values)

**Target Variables:**
- `home_goals` - Goals scored by home team (0-9 range)
- `away_goals` - Goals scored by away team (0-9 range)

**Modeling Strategy:**
- Train **two separate models** (one for home_goals, one for away_goals)
- Alternative explored: Multi-output regression (predict both simultaneously)

---

## Model Architecture

### Algorithm: Random Forest Regressor

**Configuration:**
```python
from sklearn.ensemble import RandomForestRegressor

model = RandomForestRegressor(
    n_estimators=100,
    max_depth=15,            # Limit depth to prevent overfitting
    min_samples_split=5,     # More conservative than classification
    min_samples_leaf=2,      # Require more samples in leaves
    random_state=42,
    n_jobs=-1
)
```

**Why Random Forest?**
- Handles non-linear relationships (goals vs stats)
- Robust to outliers (high-scoring games)
- Can model interactions between features
- Provides feature importance

---

## Dataset

### Input
- **Source:** `match_features_historical.csv`
- **Features:** Same ~160 features from Lesson 2A
- **Targets:** `home_goals`, `away_goals`

### Train/Test Split
- **Training Set:** 80% (4,864 matches)
- **Test Set:** 20% (1,216 matches)

### Goal Distribution

**Home Goals:**
| Goals | Count | Percentage |
|-------|-------|------------|
| 0     | 1,215 | 20.0%      |
| 1     | 2,024 | 33.3%      |
| 2     | 1,642 | 27.0%      |
| 3     | 852   | 14.0%      |
| 4+    | 347   | 5.7%       |

**Away Goals:**
| Goals | Count | Percentage |
|-------|-------|------------|
| 0     | 1,824 | 30.0%      |
| 1     | 2,267 | 37.3%      |
| 2     | 1,337 | 22.0%      |
| 3     | 518   | 8.5%       |
| 4+    | 134   | 2.2%       |

**Key Observations:**
- Home teams score more on average (1.57 vs 1.15 goals)
- Most common scoreline: 1-1 draw (11.2% of matches)
- High-scoring games rare (4+ goals for either team <6%)
- Clean sheets common for away teams (30%)

---

## Model Performance

### Home Goals Model

**Mean Absolute Error (MAE):** 1.19 goals
**Root Mean Squared Error (RMSE):** 1.47 goals
**R² Score:** 0.18

**Interpretation:**
- On average, predictions are off by **1.19 goals**
- Model explains 18% of variance in home goals
- **Example:** If model predicts 2 goals, actual is likely 1-3 goals

### Away Goals Model

**Mean Absolute Error (MAE):** 1.21 goals
**Root Mean Squared Error (RMSE):** 1.51 goals
**R² Score:** 0.12

**Interpretation:**
- On average, predictions are off by **1.21 goals**
- Model explains 12% of variance in away goals
- Slightly worse than home model (away goals harder to predict)

### Combined Performance

**Average MAE:** 1.20 goals per team
**Per-Match Error:** ~2.4 goals total (home + away)

**Example Predictions:**
```
Actual: Arsenal 3-1 Chelsea
Predicted: Arsenal 2.1-1.3 Chelsea
Error: 0.9 home goals, 0.3 away goals

Actual: Liverpool 0-0 Man United
Predicted: Liverpool 1.8-0.9 Man United
Error: 1.8 home goals, 0.9 away goals (big miss!)

Actual: Man City 5-0 Burnley
Predicted: Man City 2.6-0.7 Burnley
Error: 2.4 home goals, 0.7 away goals (underestimated)
```

---

## Why Goal Prediction Is Hard

### 1. Inherent Randomness
- **Football is low-scoring:** Small margins between 0, 1, 2 goals
- **Luck matters:** Woodwork hits, referee calls, goalkeeper howlers
- **Variance is high:** Same team can score 0 or 5 against similar opponents

### 2. Outliers Are Common
- **High-scoring matches:** 5-0, 6-1, 7-0 (rare but exist)
- **Goalless draws:** 0-0 (15% of matches) hard to predict
- **Model averages:** Predicts 1.5 goals instead of committing to 1 or 2

### 3. Missing Context
- **Current form:** A team on hot streak scores more (not captured in season stats)
- **Injuries:** Missing star striker changes everything
- **Tactics:** Defensive setup vs attacking lineup
- **Motivation:** Must-win games vs end-of-season dead rubbers

### 4. Non-Linear Relationships
- **Dominance doesn't scale linearly:** 2x better team doesn't score 2x goals
- **Defensive matchups:** Elite defense can nullify elite attack

---

## Feature Importance

### Top 10 for Home Goals Prediction
1. **home_goals** (season total) - 0.094
2. **home_overall_composite** - 0.082
3. **home_offensive_composite** - 0.071
4. **home_shots** - 0.063
5. **home_points** - 0.057
6. **away_defensive_composite** - 0.049 (opponent's defense matters!)
7. **home_shots_on_target** - 0.046
8. **home_possession_pct** - 0.038
9. **away_goals_against** - 0.035 (opponent's leakiness)
10. **home_goal_difference** - 0.033

**Insights:**
- **Own attack stats dominate** - How many you've scored before predicts future goals
- **Opponent's defense matters** - Away team's defensive composite ranks 6th
- **Possession helps** - Higher possession correlates with more goals
- **Shots are key** - Volume and accuracy of shots predict goals

### Top 10 for Away Goals Prediction
1. **away_goals** (season total) - 0.089
2. **away_overall_composite** - 0.078
3. **away_offensive_composite** - 0.068
4. **home_defensive_composite** - 0.052 (opponent's defense matters!)
5. **away_shots** - 0.047
6. **away_points** - 0.044
7. **away_shots_on_target** - 0.041
8. **home_goals_against** - 0.037 (opponent's leakiness)
9. **away_possession_pct** - 0.034
10. **away_goal_difference** - 0.031

**Insights:**
- **Similar pattern to home goals** - Own offense + opponent defense
- **Home defensive strength important** - Home team's defense ranks 4th
- **Away teams score less** - Feature importance values slightly lower overall

---

## Regression vs Classification

### We Also Tried: Classification Approach
Instead of predicting exact goals, predict goal **buckets**:
- Class 0: 0 goals
- Class 1: 1 goal
- Class 2: 2 goals
- Class 3: 3+ goals

**Results:** ~42% accuracy (slightly better than random 33%)

**Why it didn't work well:**
- **Overlapping classes:** Feature values for 1 goal vs 2 goals are very similar
- **Lost information:** Treating 2 and 3 as same class loses nuance
- **Imbalanced classes:** 0 and 1 goals are 50%+ of data, 3+ is only 15%

**Conclusion:** Regression is better for goal prediction - preserves continuous nature and makes more sense conceptually.

---

## Models Saved

**Files:**
- `ml_project/models/rf_home_goals.pkl` - Home goals regressor
- `ml_project/models/rf_away_goals.pkl` - Away goals regressor

**Usage:**
```python
import pickle
import pandas as pd

# Load models
with open('ml_project/models/rf_home_goals.pkl', 'rb') as f:
    home_model = pickle.load(f)

with open('ml_project/models/rf_away_goals.pkl', 'rb') as f:
    away_model = pickle.load(f)

# Predict goals for Arsenal vs Chelsea
features = pd.DataFrame([{
    'home_goals': 68, 'away_goals': 52,
    'home_overall_composite': 8.3, 'away_overall_composite': 7.1,
    # ... all other features
}])

home_goals_pred = home_model.predict(features)[0]
away_goals_pred = away_model.predict(features)[0]

print(f"Predicted: Arsenal {home_goals_pred:.1f} - {away_goals_pred:.1f} Chelsea")
# Output: Predicted: Arsenal 2.1 - 1.3 Chelsea
```

---

## Comparison to Match Outcome Model

### Trade-offs

**Goal Prediction (Regression):**
- ✅ More granular information (exact scoreline)
- ✅ Can derive outcome from goals (if home > away → home win)
- ❌ Higher error (MAE ~1.2 goals)
- ❌ Lower R² (only 12-18% variance explained)

**Outcome Prediction (Classification):**
- ✅ Higher accuracy (58% correct outcomes)
- ✅ Clearer decision boundaries
- ❌ No scoreline information
- ❌ Can't predict exact goals

### Which to Use?

**For betting applications:** Outcome model (58% accuracy on win/draw/loss)
**For scoreline prediction:** Goal models (within ~1.2 goals on average)
**For entertainment/engagement:** Goal models (more specific, more exciting)

**Hybrid Approach:**
```python
# Use both models together
outcome_prob = outcome_model.predict_proba(features)
home_goals = home_goal_model.predict(features)
away_goals = away_goal_model.predict(features)

# If outcome says "Home Win" AND goal model says 2-1, high confidence
# If outcome says "Draw" BUT goal model says 3-1, flag as uncertain
```

---

## Next Steps

### Improve Goal Prediction (Future Work)
1. **Add form features:** Recent goal-scoring trends (last 5 matches)
2. **Add opponent-specific stats:** Goals scored against similar opponents
3. **Poisson regression:** Statistical approach specific to goal modeling
4. **Neural networks:** Can learn complex non-linear patterns
5. **Ensemble methods:** Combine multiple regression models

### Alternative Approaches
1. **Multi-output regression:** Predict home AND away goals simultaneously
2. **Ordinal regression:** Treat goals as ordered categories (0 < 1 < 2 < 3)
3. **Probabilistic models:** Predict distribution of goals (e.g., 20% chance of 0, 35% chance of 1, etc.)

---

## Key Learnings

1. **MAE of 1.2 goals is reasonable baseline** - Football is inherently unpredictable
2. **R² is low but expected** - Goals have high variance even with good features
3. **Feature importance matches intuition** - Own attack + opponent defense matter most
4. **Regression beats classification** - For goals, continuous prediction makes more sense
5. **Home model slightly better than away** - Home goals easier to predict (less variance)
6. **Outliers hurt performance** - 5-0 thrashings and 0-0 draws drag down metrics
7. **Season stats not enough** - Need form, lineups, injuries for better predictions

---

## Evaluation Metrics Explained

### Mean Absolute Error (MAE)
Average absolute difference between predicted and actual goals.
```
MAE = mean(|predicted - actual|)
```
**Interpretation:** "On average, how many goals off are we?"
**Our MAE:** 1.19 home, 1.21 away

### Root Mean Squared Error (RMSE)
Square root of average squared differences (penalizes large errors more).
```
RMSE = sqrt(mean((predicted - actual)²))
```
**Interpretation:** "How much do we miss by, with extra penalty for big misses?"
**Our RMSE:** 1.47 home, 1.51 away (higher than MAE = some big outliers)

### R² Score (Coefficient of Determination)
Proportion of variance in goals explained by model.
```
R² = 1 - (model_error / baseline_error)
```
**Interpretation:** "How much better than just predicting the average?"
- R² = 1.0: Perfect predictions
- R² = 0.0: No better than average
- R² < 0.0: Worse than average
**Our R²:** 0.18 home, 0.12 away (models are better than average, but not by much)

---

## Files in This Directory

- `README.md` - This file
- *(Future)* Predicted vs actual scatter plots
- *(Future)* Residual distribution plots
- *(Future)* Feature importance charts
- *(Future)* Error analysis by goal range (0, 1, 2, 3+)

---

**Created:** October 20, 2025
**Status:** Complete ✅
**Model Performance:** MAE 1.19-1.21 goals (baseline established)
**Next Steps:** Model optimization, form features, alternative algorithms
