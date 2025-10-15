# Part 1B: Goals Analysis

**Date:** October 15, 2025
**Notebook:** `notebooks/01_exploratory_data_analysis/01b_goals_analysis.ipynb`
**Objective:** Explore goal-scoring patterns using descriptive statistics, comparing historical season-end data with current season progression

---

## Dataset Overview

- **Historical:** 15 complete seasons (2010-2011 through 2024-2025), GW38 totals
- **Records:** 300 squad-season combinations (15 seasons × 20 teams)
- **Current:** 2025-2026 season, GW 3-7 tracking (snapshot at GW7)

---

## Key Findings

### Overall Goal Statistics (Historical, Full Season)

| Metric | Value |
|--------|-------|
| **Mean** | 51.3 goals |
| **Median** | 49.0 goals |
| **Std Dev** | 16.8 goals |
| **Min** | 19 goals (Sheffield Utd 2020-21) |
| **Max** | 103 goals (Man City 2017-18) |
| **Range** | 84 goals |

**Distribution:** Slightly right-skewed (high-scoring outliers like Man City)

---

## Goals by Final Position Tier

### Attacking Benchmarks

| Tier | Median Goals | Mean Goals | Range |
|------|--------------|------------|-------|
| **Top 4** | 71 | 75.0 | 61-103 |
| **Upper Mid-Table (5-10)** | 55 | 56.2 | 42-71 |
| **Lower Mid-Table (11-17)** | 44 | 44.8 | 31-57 |
| **Relegation Zone (18-20)** | 36 | 36.9 | 19-54 |

### Defensive Benchmarks

| Tier | Median Conceded | Mean Conceded | Best | Worst |
|------|-----------------|---------------|------|-------|
| **Top 4** | 36 | 35.4 | 22 | 48 |
| **Upper Mid-Table (5-10)** | 49 | 48.7 | 35 | 60 |
| **Lower Mid-Table (11-17)** | 56 | 56.3 | 44 | 70 |
| **Relegation Zone (18-20)** | 65 | 66.4 | 51 | 104 |

---

## Historical Records

### Most Goals Scored (Single Season)
1. **Man City 2017-18:** 103 goals (GD: +76, 100 pts) - Centurions season
2. **Man City 2019-20:** 100 goals (GD: +65, 81 pts)
3. **Liverpool 2013-14:** 97 goals (GD: +47, 84 pts) - Suarez/Sturridge era

### Fewest Goals Scored (Single Season)
1. **Sheffield Utd 2020-21:** 19 goals (GD: -44, 23 pts) - Worst attack in dataset
2. **Norwich City 2021-22:** 21 goals (GD: -63, 22 pts)
3. **Huddersfield 2018-19:** 21 goals (GD: -55, 16 pts)

### Best Defense (Fewest Conceded)
1. **Liverpool 2018-19:** 22 conceded (GD: +64, 97 pts) - Van Dijk masterclass
2. **Man City 2018-19:** 23 conceded (GD: +68, 98 pts) - Title winners
3. **Man City 2021-22:** 26 conceded (GD: +70, 93 pts)

### Worst Defense (Most Conceded)
1. **Sheffield Utd 2023-24:** 104 conceded (GD: -73, 16 pts) - Worst in dataset
2. **Southampton 2024-25:** 86 conceded (GD: -61, 12 pts)
3. **Luton Town 2023-24:** 85 conceded (GD: -36, 26 pts)

---

## Current Season Analysis (GW7, 2025-2026)

### Top Scorers (GW7)
1. **Arsenal:** 14 goals (2.0 goals/game, projected: 76 goals)
2. Other teams tracking below historical Top 4 pace

### Best Defenses (GW7)
1. **Arsenal:** 3 conceded (0.43/game, projected: 16 goals)

### Season Projections
- **1 team** on Top 4 attacking pace (71+ goals projected)
- **13 teams** on relegation pace (<54 goals projected)
- **Note:** Early season, small sample size (7 games) - projections will stabilize

---

## Big 6 Analysis (2010-2025)

### Average Performance Over 15 Seasons

| Team | Avg Position | Best | Worst | Avg Points |
|------|--------------|------|-------|------------|
| **Man City** | 1.8 | 1st | 4th | 83.7 |
| **Liverpool** | 4.1 | 1st | 8th | 74.5 |
| **Arsenal** | 4.2 | 2nd | 8th | 71.8 |
| **Chelsea** | 4.5 | 1st | 12th | 69.8 |
| **Man Utd** | 4.7 | 1st | 15th | 69.9 |
| **Tottenham** | 5.6 | 2nd | 17th | 66.4 |

### Notable Underperformances (Big 6 Outside Top 6)

**2024-2025 Season:**
- **Tottenham:** 17th place (38 pts, GD: -4) - Catastrophic collapse
- **Man Utd:** 15th place (42 pts, GD: -12) - Worst finish in modern era

**Previous Disasters:**
- **Chelsea 2022-23:** 12th (44 pts) - Post-Abramovich era struggles
- **Chelsea 2015-16:** 10th (50 pts) - Mourinho meltdown
- **Arsenal 2019-21:** Back-to-back 8th place finishes

### Championship Wins (2010-2025)
- **Man City:** 9 titles (dominance era)
- **Liverpool:** 2 titles (2019-20 with 99 pts, 2024-25 with 84 pts)
- **Chelsea:** 2 titles (2014-15, 2016-17)
- **Man Utd:** 1 title (2012-13, last title)
- **Arsenal:** 0 titles (best: 2nd place multiple times)
- **Tottenham:** 0 titles (best: 2nd in 2016-17)

---

## Key Insights for Modeling

### 1. Goal Scoring Strongly Predicts Position
- Clear separation between tiers
- Top 4 teams average **24 more goals** than mid-table (75 vs 51)
- Relegated teams score **38 fewer goals** than Top 4 (37 vs 75)

### 2. Defense Matters for Top 4
- Top 4 teams concede **≤36 goals** on average
- **20-goal defensive advantage** over mid-table teams
- Elite defense (≤25 conceded) almost guarantees Top 4

### 3. Goal Difference Strongly Correlates
- Top 4 average GD: +39
- Relegation average GD: -29
- 68-goal difference swing between success and failure

### 4. High Variance in Relegation Zone
- Relegated teams range from 19-54 goals scored
- Some teams go down fighting (54 goals), others collapse (19 goals)
- Defense tends to be consistently poor (51-104 conceded)

### 5. Early Season Projections Unreliable
- GW7 projections show 13 teams on relegation pace
- Small sample size creates extreme projections
- Need at least 10-15 games for stable metrics

---

## Visualizations Generated

1. **historical_goals_distribution.png** - Histogram + box plot of goals scored distribution
2. **goals_by_position_tier.png** - Box plots comparing goals by final position tier
3. **current_goals_leaders.png** - Bar chart of current season goals at GW7
4. **goals_scatter_historical.png** - Goals scored vs conceded, colored by final tier
5. **goals_analysis_summary.txt** - Text summary of key findings

---

## Next Steps

**Part 1C: Composite Scores Analysis**
- Analyze overall team performance metrics
- Investigate defensive vs offensive balance
- Compare form indicators across position tiers

---

## Methodology Notes

- **Data Source:** `analytics_squads` table (DuckDB)
- **Calculations:** Points = (Wins × 3) + Draws, Goal Difference = Goals - Goals Against
- **Tiers:** Top 4 (1-4), Upper Mid (5-10), Lower Mid (11-17), Relegation (18-20)
- **Position Rankings:** Within-season ranking by points, then goal difference
- **Statistical Tests:** Skewness, kurtosis checks for distribution shape
