# Future Features & Enhancements

## Phase 2: Lineup-Based Predictions (Lesson 4)

### Objective
Enhance match predictor with starting XI data for improved accuracy.

### Data Requirements
1. Scrape lineup data from FBRef
   - Starting XI per match
   - Substitutions
   - Player minutes played
   
2. Add `match_lineups` table to DuckDB
   - Schema: match_id, team_id, player_id, position, is_starter, minutes

3. Link lineups to match outcomes
   - Join match_lineups → analytics_fixtures
   - Calculate starting XI aggregate stats per match

### Feature Engineering
1. **Lineup Quality Score**
   - Sum of starting XI player ratings
   - Compare home XI quality vs away XI quality
   
2. **Key Player Presence**
   - Binary: Is team's top scorer starting?
   - Binary: Is team's best defender starting?
   
3. **Squad Rotation Indicator**
   - Changes from previous match lineup
   - Fatigue indicator (minutes in last 7 days)

4. **Position-Specific Matchups**
   - Home striker quality vs away defense quality
   - Home midfield vs away midfield

### ML Model Enhancement
- Train model with lineup features
- Compare accuracy: squad-only vs squad+lineup
- Expected improvement: +3-5% accuracy

### Dashboard Feature: Manual Lineup Entry
```
[Arsenal ▼]  vs  [Chelsea ▼]

☑ Manually Enter Lineups

Arsenal Starting XI:
GK:  [Ramsdale ▼]
DEF: [Saliba ▼] [Gabriel ▼] [White ▼] [Zinchenko ▼]
MID: [Rice ▼] [Odegaard ▼] [Havertz ▼]
FWD: [Saka ▼] [Jesus ▼] [Martinelli ▼]

[PREDICT MATCH]
```

**Output:**
- Win probability with lineup adjustment
- Key lineup advantages/disadvantages
- Predicted score with lineup context

### Success Criteria
- Lineup features improve model accuracy by ≥3%
- Manual lineup entry works smoothly in dashboard
- Predictions update in real-time when lineup changes

---

## Phase 3: Auto-Populated Lineups (Lesson 6+)

### Objective
Automatically predict likely lineups based on patterns.

### Data Requirements
1. Historical lineup data (Phase 2 requirement)
2. Injury/suspension data (new scraping needed)
3. Press conference hints (optional, advanced)

### Approach
Train a "lineup prediction model":
- Input: Recent starting XI, injuries, opponent strength, competition
- Output: Predicted starting XI for next match

### Integration
- User clicks "Auto-fill Lineups"
- System predicts both teams' starting XIs
- User can review/edit before predicting match

---

## Phase 4: Player-vs-Player Matchups (Advanced)

### Objective
Model individual positional battles.

### Examples
- Haaland vs Van Dijk
- Saka vs Cucurella
- Odegaard vs Casemiro

### Approach
- Extract player pairwise stats when they face each other
- Train model: (player_A_stats, player_B_stats) → outcome
- Aggregate all 22 player matchups → team prediction

### Complexity
HIGH - requires significant data engineering and advanced ML.

---

## Other Future Ideas

### 1. Live Match Updates
- Update predictions in real-time as match progresses
- Adjust based on goals, red cards, subs

### 2. Historical "What-If" Analysis
- "What if Arsenal had started Nketiah instead of Jesus?"
- Retroactively test lineup decisions

### 3. Optimal Lineup Recommender
- "Which starting XI maximizes win probability vs Chelsea?"
- Try all lineup permutations, rank by predicted outcome

### 4. Injury Impact Quantification
- "Arsenal loses 8% win probability without Saka"
- Quantify each player's impact on team performance

---

## Implementation Priority

1. ✅ **Phase 1:** Squad-level model (Lessons 2-3)
2. 🔄 **Phase 2:** Lineup predictions (Lesson 4)
3. ⏭️ **Phase 3:** Auto-lineups (Lesson 6+)
4. ⏭️ **Phase 4:** Player matchups (Advanced)
5. ⏭️ **Other:** As needed based on user feedback

---

**Last Updated:** October 20, 2025