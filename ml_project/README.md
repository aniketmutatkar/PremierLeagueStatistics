# Premier League ML Project

Machine learning experimentation and model development for Premier League match prediction.

## Structure
- `notebooks/`: Jupyter notebooks for exploration and analysis
- `outputs/`: Generated charts, reports, and analysis results
- `data/`: ML-specific data artifacts (processed datasets, features, splits)
- `models/`: Saved trained models (future)
- `scripts/`: Reusable ML utility functions (future)

## Current Status

### Lesson 1: Exploratory Data Analysis (EDA)
- [x] **Part 1A: Data Inspection** - Database schema, data quality, completeness analysis
- [x] **Part 1B: Goals Analysis** - Historical goal patterns, position tiers, current season projections
- [ ] Part 1C: Composite Scores Analysis
- [ ] Part 1D: Form & Momentum
- [ ] Part 1E: Home vs Away Performance
- [ ] Part 1F: Head-to-Head Patterns
- [ ] Part 1G: Advanced Metrics (xG, shot accuracy, etc.)
- [ ] Part 1H: Correlation Analysis

### Future Lessons
- [ ] Lesson 2: Feature engineering
- [ ] Lessons 3+: Model development

## Completed Analyses

### Part 1A: Data Inspection (Oct 15, 2025)
**Location:** `outputs/01_data_inspection/`
- Analyzed 15 complete seasons (2010-2025) + current season (2025-2026)
- Verified 300 historical squad records at GW38
- Identified data quality issues (gameweek variations)
- Generated schema documentation and sample data

### Part 1B: Goals Analysis (Oct 15, 2025)
**Location:** `notebooks/01_exploratory_data_analysis/01b_goals_analysis.ipynb`
**Outputs:** `outputs/02_goals_analysis/`

**Key Findings:**
- **Average full season:** 51.3 goals scored, 51.3 goals conceded
- **Top 4 threshold:** 71+ goals scored, ≤36 goals conceded
- **Relegation threshold:** <54 goals scored
- **Current leaders (GW7):** Arsenal (14 goals, 3 conceded)
- **Historical records:**
  - Most goals: Man City 2017-18 (103 goals, 100 pts)
  - Best defense: Liverpool 2018-19 (22 conceded, 97 pts)
  - Worst attack: Sheffield Utd 2020-21 (19 goals)
  - Worst defense: Sheffield Utd 2023-24 (104 conceded)
