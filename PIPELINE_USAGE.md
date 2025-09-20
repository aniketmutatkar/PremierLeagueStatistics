# Pipeline Usage Guide

## 🚀 Production Pipelines

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
python validate_analytics_system.py
```

## 🏗️ System Architecture

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

## 📊 Data Architecture

### Two-Database System
- **Raw Database** (`premierleague_raw.duckdb`): Preserves original FBRef structure (33 tables)
- **Analytics Database** (`premierleague_analytics.duckdb`): Unified analytics with SCD Type 2 (4 tables)

### Unified Analytics Tables
**Complete Entity Coverage (NEW):**
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

## 🎯 Pipeline Details

### Master Pipeline Intelligence
The master pipeline uses sophisticated decision-making to optimize operations:

```bash
# Check what the pipeline would do
python pipelines/master_pipeline.py --status
```

**Decision Logic:**
1. **Analytics behind Raw**: Runs analytics pipeline only
2. **New gameweek detected**: Runs both raw and analytics pipelines
3. **All current**: Skips both pipelines unless forced
4. **Force flags**: Override intelligent decisions

**Example Status Output:**
```
📊 Current Status:
   Raw gameweek: 5
   Analytics gameweek: 5
   Analytics entities: 444 total (380 players + 24 keepers + 20 squads + 20 opponents)
   Refresh needed: ❌ No
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
1. **Data Consolidation**: Uses single `DataConsolidator` for all entity types
2. **Column Mapping**: Maps FBRef columns to clean analytics names consistently
3. **SCD Type 2 Processing**: Handles historical tracking across all entities
4. **Data Validation**: Comprehensive quality checks for all 4 analytics tables

**Performance:**
- Processes ~410 entities in ~1 second
- 100% data coverage (vs. 33% in previous versions)
- Unified processing logic for consistency

## 🔍 System Validation

### Comprehensive Validation
```bash
python validate_analytics_system.py
```

**Validation Coverage:**
1. **Schema Validation**: Table structure and column presence for all 4 tables
2. **SCD Type 2 Integrity**: Historical tracking correctness across all entities
3. **Data Quality Validation**: Missing data and logical consistency checks
4. **Cross-Entity Validation**: Squad/opponent/player relationship verification
5. **Business Logic Validation**: Statistical sanity checks and Premier League constraints

**Example Validation Output:**
```
================================================================================
VALIDATION SUMMARY
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

## ⚙️ Configuration Management

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

## 📝 Error Handling & Logging

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

## 🎮 Advanced Usage

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
1. **Test individual components**: Run validation frequently during development
2. **Check status before running**: Use `--status` flags to understand current state
3. **Use dry-run for planning**: Preview operations with `--dry-run`
4. **Monitor logs**: Check timestamped logs for detailed progress tracking
5. **Validate after changes**: Always run validation after code modifications

## 🔬 Data Science Integration

### Accessing Unified Analytics Data
```python
import duckdb

# Connect to unified analytics database
conn = duckdb.connect('data/premierleague_analytics.duckdb')

# Query current top scorers
current_players = conn.execute("""
    SELECT player_name, squad_name, goals, assists, expected_goals
    FROM analytics_players 
    WHERE is_current = true
    ORDER BY goals DESC
    LIMIT 10
""").fetchdf()

# Query team performance
team_stats = conn.execute("""
    SELECT squad_name, goals, shots, expected_goals, defensive_actions
    FROM analytics_squads 
    WHERE is_current = true 
    ORDER BY goals DESC
""").fetchdf()

# Historical player progression (SCD Type 2)
player_progression = conn.execute("""
    SELECT gameweek, player_name, squad_name, goals, valid_from, valid_to
    FROM analytics_players 
    WHERE player_name = 'Erling Haaland'
    ORDER BY gameweek
""").fetchdf()

# Goalkeeper analysis
keeper_performance = conn.execute("""
    SELECT player_name, squad_name, save_percentage, clean_sheets, post_shot_xg_performance
    FROM analytics_keepers 
    WHERE is_current = true AND minutes_played > 270
    ORDER BY save_percentage DESC
""").fetchdf()
```

### Analysis Capabilities
- **Player Analysis**: 404+ tracked players with complete performance history
- **Team Analytics**: 20 Premier League squads with comprehensive tactical data
- **Opposition Scouting**: 20 opponent profiles for strategic analysis  
- **Transfer Impact Analysis**: Automatic detection using SCD Type 2 data
- **Performance Trends**: Multi-gameweek analysis across all entity types
- **Cross-Entity Insights**: Player performance in team context

## 🛠️ Troubleshooting

### Common Issues
1. **Import errors after updates**: Clear Python cache with `find . -name "*.pyc" -delete`
2. **Database connection issues**: Check paths in `config/database.yaml`
3. **Scraping failures**: Check internet connection and FBRef availability
4. **SCD validation failures**: Run analytics pipeline with `--force` flag
5. **Missing analytics tables**: Run `python create_analytics_db.py` to rebuild

### Performance Tips
- **Use master pipeline**: More efficient than running individual pipelines
- **Monitor log file sizes**: Logs rotate automatically but check disk space
- **Regular validation**: Run validation weekly to catch data quality issues early
- **Database optimization**: Analytics database automatically optimizes queries with proper indexing

### Debug Commands
```bash
# Check system health
python pipelines/master_pipeline.py --status

# Validate without processing
python pipelines/analytics_pipeline.py --validate

# Full system validation
python validate_analytics_system.py

# Check recent pipeline logs
tail -f data/logs/master_pipeline_*.log
```

---

**For detailed technical implementation and system architecture, see the main README.md**