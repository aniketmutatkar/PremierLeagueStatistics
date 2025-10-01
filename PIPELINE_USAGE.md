# Pipeline Usage Guide

## Production Pipelines

### Master Pipeline (Recommended)
```bash
# Smart run - only executes when needed
python pipelines/master_pipeline.py

# Force refresh both raw and analytics
python pipelines/master_pipeline.py --force-all

# Force only raw data pipeline
python pipelines/master_pipeline.py --force-raw

# Force only analytics pipeline
python pipelines/master_pipeline.py --force-analytics

# Check comprehensive system status
python pipelines/master_pipeline.py --status

# Preview what operations would run
python pipelines/master_pipeline.py --dry-run
```

### Individual Pipelines
```bash
# Raw data scraping pipeline
python pipelines/raw_pipeline.py                # Smart run
python pipelines/raw_pipeline.py --force        # Force refresh
python pipelines/raw_pipeline.py --status       # Check raw status

# Unified analytics ETL pipeline  
python pipelines/analytics_pipeline.py          # Smart run
python pipelines/analytics_pipeline.py --force  # Force refresh
python pipelines/analytics_pipeline.py --status # Check analytics status
python pipelines/analytics_pipeline.py --validate # Validation only
```

### System Validation
```bash
# Comprehensive validation of entire unified system
python scripts/validate_analytics_system.py
```

## System Architecture

### Project Structure
```
PremierLeagueStatistics/
├── pipelines/                          # Production pipelines
│   ├── master_pipeline.py              # Intelligent orchestration
│   ├── raw_pipeline.py                 # FBRef scraping pipeline
│   └── analytics_pipeline.py           # Unified analytics ETL
├── src/                                # Core library code
│   ├── analytics/                      # Unified analytics components
│   │   ├── analytics_etl.py            # Main ETL engine
│   │   ├── data_consolidation.py       # Unified data consolidator
│   │   ├── scd_processor.py            # SCD Type 2 processor
│   │   └── column_mappings.py          # FBRef → Analytics mapping
│   ├── database/                       # Database connections & operations
│   │   ├── raw_db/                     # Raw database operations
│   │   └── analytics_db/               # Analytics database operations
│   └── scraping/                       # FBRef scraping components
├── config/                             # Configuration files
│   ├── sources.yaml                    # FBRef URL mappings
│   ├── scraping.yaml                   # Rate limiting settings
│   ├── database.yaml                   # Database configuration
│   └── pipeline.yaml                   # Pipeline settings
├── data/                               # Data storage
│   ├── logs/                           # Timestamped pipeline logs
│   ├── backups/                        # Database backups
│   ├── premierleague_raw.duckdb        # Raw FBRef data (33 tables)
│   └── premierleague_analytics.duckdb  # Unified analytics (4 tables)
├── notebooks/                          # Data science notebooks
└── docs/                               # Documentation
```

## Fixture-Based Gameweek System

### How It Works
The system calculates gameweeks **per team** based on completed fixtures, not a single global gameweek.

**Key Concept**: 
- Each team's gameweek = `MAX(gameweek)` where their fixtures have `is_completed = true`
- Different teams can be at different gameweeks simultaneously
- Raw data has NO gameweek column (gameweek assigned in analytics layer)

### Data Flow
```
1. Scraper → Fixtures table (with is_completed flag)
2. Analytics ETL → Calculate team gameweeks from fixtures
3. Consolidation → Assign each record its team's gameweek
4. SCD Processor → Update only teams with new data
5. Master Pipeline → Compare team-by-team to detect updates
```

### Real-World Scenarios

**Scenario 1: Postponement**
```
Situation: Man City vs Burnley (GW6) postponed
Result in Analytics:
  - Man City: gameweek = 5, matches_played = 5
  - Burnley: gameweek = 5, matches_played = 5
  - Other 18 teams: gameweek = 6, matches_played = 6
```

**Scenario 2: Mid-Gameweek Scraping**
```
Situation: Scrape on Saturday (10 teams played, 10 haven't)
Result in Analytics:
  - 10 teams completed GW6: gameweek = 6
  - 10 teams not yet played GW6: gameweek = 5
Next scrape: System updates only the 10 teams that completed GW6
```

**Scenario 3: Rescheduled Match**
```
Situation: Man City plays postponed GW6 match
Master Pipeline Detection:
  Raw: Man City now at GW6
  Analytics: Man City still at GW5
  Decision: Update only Man City (not other 19 teams)
```

### Why This Approach?

**Old System (Single Global Gameweek)**:
- Used ONE number for ALL teams
- Broke with postponements
- Couldn't handle mid-gameweek scraping
- Updated all 20 teams even if only 1 had new data

**New System (Team-Specific Gameweeks)**:
- Each team has their own gameweek based on fixtures
- Handles postponements naturally
- Accurate during mid-gameweek scraping
- Only updates teams with new completed fixtures
- Matches real-world Premier League complexity

## Data Architecture

### Two-Database System
- **Raw Database** (`premierleague_raw.duckdb`): Preserves original FBRef structure (33 tables)
- **Analytics Database** (`premierleague_analytics.duckdb`): Unified analytics with SCD Type 2 (4 tables)

### Unified Analytics Tables
**Complete Entity Coverage:**
- **`analytics_players`**: 154 columns, outfield player statistics
- **`analytics_keepers`**: 64 columns, goalkeeper-specific metrics
- **`analytics_squads`**: 185 columns, team-level analytics  
- **`analytics_opponents`**: 185 columns, opposition analysis

### SCD Type 2 Implementation
- **Complete historical tracking** across all entity types (players, keepers, squads, opponents)
- **Transfer detection and impact analysis** with automatic squad change tracking
- **Performance progression** tracking over time for all entities
- **Dynamic season detection** with proper historical versioning
- **Unified SCD processor** handles all entity types consistently

## Pipeline Details

### Master Pipeline Intelligence (Team-Specific Mode)

The master pipeline compares gameweeks **team-by-team** rather than globally:

```bash
python pipelines/master_pipeline.py --status
```

**Example Output**:
```
📊 Current Status:
   Raw data: GW5-6 across 20 teams (2 teams behind)
   Analytics data: GW5-5 across 20 teams
   Teams needing update: Man City (GW5→6), Burnley (GW5→6)
   Refresh needed: ✅ Yes (2 teams)
```

**Decision Logic**:
1. Calculate team gameweeks from raw_fixtures
2. Get current team gameweeks from analytics_players
3. Compare team-by-team:
   ```python
   for team in all_teams:
       if raw_gw[team] > analytics_gw[team]:
           teams_to_update.append(team)
   ```
4. If any teams need updating → run analytics pipeline
5. SCD processor marks ONLY those teams as historical

**Efficiency Example**:
```
Scenario: Man City completes postponed match
Raw: 19 teams at GW6, Man City at GW6 (was GW5)
Analytics: 19 teams at GW6, Man City at GW5
Master Pipeline: Detects only Man City needs update
Analytics Pipeline: Updates only Man City (skip other 19 teams)
SCD Processor: Marks only Man City GW5 as historical
```

### Raw Pipeline
Scrapes data from FBRef using the proven "archive pattern":

```bash
# Normal operation
python pipelines/raw_pipeline.py

# Check status without running
python pipelines/raw_pipeline.py --status

# Force refresh even if current
python pipelines/raw_pipeline.py --force
```

**Features:**
- Scrapes all 33 FBRef stat tables (11 categories × 3 entity types)
- Automatic gameweek detection from fixtures
- Rate limiting with respectful delays (10 seconds between requests)
- Archive-pattern data cleaning (proven method)
- Comprehensive error handling and retry logic

**Note on Gameweek Tagging**:
The raw pipeline stores fixtures with completion status but does NOT tag stat tables with gameweeks. You'll see this in the logs:
```
✅ 33 tables populated
💡 NOTE: Raw data stored WITHOUT gameweek tagging
   Gameweek assignment will happen in analytics layer
```

This is intentional. Gameweeks are calculated dynamically in the analytics layer based on each team's completed fixtures.

### Unified Analytics Pipeline
Processes raw data into clean analytics tables with SCD Type 2 across all entity types:

```bash
# Normal operation
python pipelines/analytics_pipeline.py

# Force complete refresh
python pipelines/analytics_pipeline.py --force

# Status check
python pipelines/analytics_pipeline.py --status

# Validation only (no data processing)
python pipelines/analytics_pipeline.py --validate
```

**Unified ETL Process:**
1. **Team Gameweek Calculation**: Calculate each team's gameweek from raw_fixtures
2. **Data Consolidation**: Uses single `DataConsolidator` for all entity types
3. **Gameweek Assignment**: Assign team-specific gameweeks to each record
4. **SCD Processing**: Group by gameweek, process each group separately
5. **Historical Marking**: Mark only updated teams as historical (not all teams)
6. **Validation**: Comprehensive data quality checks

**Pipeline Output Example:**
```
✅ Consolidated 464 total entities
   - Outfield: 398 players
   - Goalkeepers: 26 players
   - Squads: 20 squads
   - Opponents: 20 opponents
🎯 Gameweeks assigned: GW6 (all teams aligned)
✅ All SCD Type 2 processing completed
🔍 Analytics data validation passed
🎉 ETL Pipeline completed successfully in 1.3s
```

## Understanding Gameweek Behavior

### What You'll See in Analytics

**Query Example**:
```sql
SELECT squad, gameweek, COUNT(*) as players
FROM analytics_players
WHERE is_current = true
GROUP BY squad, gameweek
ORDER BY gameweek, squad;
```

**Normal Result (All Teams Aligned)**:
```
Arsenal: GW6, 20 players
Brighton: GW6, 19 players
Chelsea: GW6, 22 players
...
(All 20 teams at GW6)
```

**Result During Postponement**:
```
Man City: GW5, 20 players     ← Postponed GW6
Burnley: GW5, 19 players      ← Postponed GW6
Arsenal: GW6, 20 players
Brighton: GW6, 19 players
...
(18 teams at GW6, 2 teams at GW5)
```

### Common Questions

**Q: Why do teams have different gameweeks?**
A: Real-world postponements, rescheduling, or mid-gameweek scraping. This is accurate behavior.

**Q: Will teams "catch up" eventually?**
A: Yes. When postponed matches are played and you re-scrape, those teams' gameweeks will increment.

**Q: Does this affect historical data?**
A: Yes, positively. Each team's historical records reflect their actual match progression, not a fake global gameweek.

**Q: What about matches_played vs gameweek?**
A: They should be equal or close (within 1-2). Large differences indicate data quality issues or players who haven't played much.

## System Validation

### Comprehensive Validation
```bash
python scripts/validate_analytics_system.py
```

**Validation Coverage:**
- Schema validation across all analytics tables
- SCD Type 2 integrity checks
- Data quality validation
- Cross-entity validation
- Business logic validation

**Example Output:**
```
================================================================================
ANALYTICS SYSTEM VALIDATION
================================================================================
Schema Validation........................................... PASS
SCD Type 2 Validation....................................... PASS
Data Quality Validation..................................... PASS
Cross-Entity Validation..................................... PASS
Business Logic Validation................................... PASS
================================================================================
OVERALL RESULT: ALL VALIDATIONS PASSED
================================================================================
```

## Configuration Management

### Core Configuration Files

#### sources.yaml
Maps FBRef URLs to stat categories:
```yaml
stats_sources:
  standard:
    url: "https://fbref.com/en/comps/9/stats/Premier-League-Stats"
    tables: ["squad_standard", "opponent_standard", "player_standard"]
  shooting:
    url: "https://fbref.com/en/comps/9/shooting/Premier-League-Stats"
    tables: ["squad_shooting", "opponent_shooting", "player_shooting"]
```

#### scraping.yaml
Controls scraping behavior:
```yaml
scraping:
  delays:
    between_requests: 10  # Respectful to FBRef
    connection_timeout: 30
  retries:
    max_attempts: 3
    backoff_factor: 2
```

#### database.yaml
Database paths and settings:
```yaml
database:
  paths:
    raw: "data/premierleague_raw.duckdb"
    analytics: "data/premierleague_analytics.duckdb"
  backup:
    enabled: true
    retention_days: 30
```

#### pipeline.yaml
Pipeline orchestration settings:
```yaml
pipeline:
  master:
    timeout_raw: 1800      # 30 minutes
    timeout_analytics: 600 # 10 minutes
  analytics:
    scd_processing: true
    validation_enabled: true
```

## Error Handling & Logging

### Enhanced Logging
All pipelines use timestamped, rotated logging:

```bash
# View recent logs
ls -la data/logs/

# Example log files
master_pipeline_20250920_140151.log
analytics_pipeline_20250920_140155.log
raw_pipeline_20250920_140130.log
```

### Error Recovery
- **Non-critical errors**: Pipeline continues with warnings
- **Critical errors**: Pipeline stops with detailed error messages
- **Timeout handling**: Reasonable timeouts for each pipeline stage
- **Retry logic**: Built-in retry for transient failures
- **Validation gates**: Comprehensive checks before committing data

## Advanced Usage

### Force Operations
```bash
# Force only raw pipeline (useful for FBRef updates)
python pipelines/master_pipeline.py --force-raw

# Force only analytics pipeline (useful for schema changes)
python pipelines/master_pipeline.py --force-analytics

# Force both pipelines (complete refresh)
python pipelines/master_pipeline.py --force-all
```

### Status Monitoring
```bash
# System-wide status with entity counts
python pipelines/master_pipeline.py --status

# Individual pipeline detailed status
python pipelines/raw_pipeline.py --status
python pipelines/analytics_pipeline.py --status

# Quick validation check
python pipelines/analytics_pipeline.py --validate
```

### Development Workflow
1. Make code changes in `src/`
2. Test with `--dry-run` to preview
3. Run with `--force-all` to test changes
4. Validate with `python scripts/validate_analytics_system.py`
5. Check logs in `data/logs/` for detailed output

## Troubleshooting

### Common Issues

**Import errors after updates**: 
```bash
find . -name "*.pyc" -delete
```

**Database connection issues**: 
Check paths in `config/database.yaml`

**Scraping failures**: 
Check internet connection and FBRef availability

**SCD validation failures**: 
```bash
python pipelines/analytics_pipeline.py --force
```

**Missing analytics tables**: 
```bash
python scripts/create_analytics_db.py
```

### Gameweek-Related Issues

**Teams at different gameweeks**:
- **Not an error**: This is normal during postponements or mid-gameweek scraping
- **Verify**: Check `raw_fixtures` table for `is_completed` status
- **Expected**: System will align once all teams play their matches

**Pipeline says "Teams needing update" but gameweeks look aligned**:
- **Cause**: You may have scraped mid-processing of a gameweek
- **Fix**: Re-run `python pipelines/master_pipeline.py` - it will detect no updates needed
- **Prevention**: Run pipeline after gameweeks are fully complete

**Historical records missing for some teams**:
- **Cause**: SCD processor only marks updated teams as historical
- **Expected**: Teams without new data keep their current records
- **Not a bug**: This is efficient selective processing

**Matches_played doesn't match gameweek**:
- **Normal**: Rotation players, substitutes, injured players have fewer matches
- **Check**: Look at MAX(matches_played) per team, not individual players
- **Issue only if**: MAX(matches_played) > gameweek (player played more matches than team completed)

### Debug Commands
```bash
# Check system health
python pipelines/master_pipeline.py --status

# Validate without processing
python pipelines/analytics_pipeline.py --validate

# Full system validation
python scripts/validate_analytics_system.py

# Check recent pipeline logs
tail -f data/logs/master_pipeline_*.log
```

## Performance Tips

- **Use master pipeline**: More efficient than running individual pipelines
- **Monitor log file sizes**: Logs rotate automatically but check disk space
- **Regular validation**: Run validation weekly to catch data quality issues early
- **Database optimization**: Analytics database automatically optimizes queries with proper indexing

---

**For detailed technical implementation and system architecture, see the main README.md**