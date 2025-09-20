# Pipeline Usage

## Production Pipelines

### Master Pipeline (Recommended)
```bash
# Run complete pipeline (raw + analytics)
python pipelines/master_pipeline.py

# Force refresh both pipelines
python pipelines/master_pipeline.py --force-all

# Check system status
python pipelines/master_pipeline.py --status

# Preview what would run
python pipelines/master_pipeline.py --dry-run
```

### Individual Pipelines
```bash
# Raw data pipeline only
python pipelines/raw_pipeline.py

# Analytics pipeline only  
python pipelines/analytics_pipeline.py

# Force analytics refresh
python pipelines/analytics_pipeline.py --force
```

### System Validation
```bash
# Comprehensive system validation
python validate_analytics_system.py
```

## Project Structure
```
├── pipelines/                 # Production pipelines
│   ├── master_pipeline.py     # Orchestrates raw + analytics  
│   ├── raw_pipeline.py        # FBRef scraping pipeline
│   └── analytics_pipeline.py  # SCD Type 2 analytics ETL
├── src/                       # Core library code
│   ├── analytics/             # Analytics components (flattened)
│   │   ├── analytics_etl.py   # Main ETL engine
│   │   ├── player_consolidation.py # Data consolidation
│   │   ├── scd_processor.py   # SCD Type 2 processor
│   │   └── column_mappings.py # FBRef → Analytics mapping
│   ├── database/              # Database connections & operations
│   │   ├── raw_db/            # Raw database operations
│   │   └── analytics_db/      # Analytics database operations
│   └── scraping/              # FBRef scraping components
├── config/                    # Configuration files
│   ├── sources.yaml           # FBRef URL mappings
│   ├── scraping.yaml          # Rate limiting settings
│   ├── database.yaml          # Database configuration
│   └── pipeline.yaml          # Pipeline settings
├── data/                      # Data storage
│   ├── logs/                  # Pipeline logs
│   └── backups/               # Database backups
├── notebooks/                 # Data science notebooks
└── docs/                      # Documentation
```

## Data Architecture

### Two-Database System
- **Raw Database** (`premierleague_raw.duckdb`): Preserves original FBRef structure
- **Analytics Database** (`premierleague_analytics.duckdb`): SCD Type 2 with 160+ columns

### SCD Type 2 Implementation
- Complete historical tracking of all players
- Transfer detection and impact analysis  
- Performance progression over time
- Dynamic season detection
- Centralized SCD processor handles both outfield players and goalkeepers

## Pipeline Details

### Master Pipeline Intelligence
The master pipeline uses smart decision-making to avoid unnecessary operations:

```bash
# Check if pipelines need to run
python pipelines/master_pipeline.py --status
```

**Decision Logic:**
1. **Analytics behind Raw**: Runs analytics pipeline only
2. **New gameweek detected**: Runs both raw and analytics pipelines
3. **All current**: Skips both pipelines
4. **Force flags**: Override intelligent decisions

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
- Scrapes all 33 FBRef stat tables (11 categories × 3 types)
- Automatic gameweek detection from fixtures
- Rate limiting with respectful delays
- Archive-pattern data cleaning (proven method)

### Analytics Pipeline
Processes raw data into clean analytics tables with SCD Type 2:

```bash
# Normal operation
python pipelines/analytics_pipeline.py

# Force refresh
python pipelines/analytics_pipeline.py --force

# Status check
python pipelines/analytics_pipeline.py --status

# Validation only
python pipelines/analytics_pipeline.py --validate
```

**ETL Process:**
1. **Data Consolidation**: Merges 11 player stat tables into outfield/keeper datasets
2. **Column Mapping**: Maps FBRef columns to clean analytics names
3. **SCD Type 2 Processing**: Handles historical tracking and transfers
4. **Data Validation**: Comprehensive quality checks

### System Validation
Comprehensive validation of the entire system:

```bash
python validate_analytics_system.py
```

**Validation Tests:**
1. **Schema Validation**: Table structure and column presence
2. **SCD Type 2 Integrity**: Historical tracking correctness
3. **Player Tracking**: Transfer detection across gameweeks
4. **Data Consolidation**: Column mapping success rates
5. **Data Quality**: Logical consistency and completeness
6. **Statistical Validation**: Sanity checks on player stats
7. **Summary Statistics**: Overall system health metrics

## Configuration Management

### Core Configuration Files

#### sources.yaml
Maps FBRef URLs to stat categories:
```yaml
stats_sources:
  standard:
    url: "https://fbref.com/en/comps/9/stats/Premier-League-Stats"
    tables: ["squad_standard", "opponent_standard", "player_standard"]
```

#### scraping.yaml
Controls scraping behavior:
```yaml
scraping:
  delays:
    between_requests: 10  # Respectful to FBRef
```

#### database.yaml
Database paths and settings:
```yaml
database:
  paths:
    raw: "data/premierleague_raw.duckdb"
    analytics: "data/premierleague_analytics.duckdb"
```

#### pipeline.yaml
Pipeline orchestration settings:
```yaml
pipeline:
  master:
    timeout_raw: 1800      # 30 minutes
    timeout_analytics: 600 # 10 minutes
```

## Error Handling & Logging

### Enhanced Logging
All pipelines use timestamped, rotated logging:

```bash
# View recent logs
ls -la data/logs/

# Example log files
master_pipeline_20240920_100151.log
analytics_pipeline_20240920_100155.log
raw_pipeline_20240920_100130.log
```

### Error Recovery
- **Non-critical errors**: Pipeline continues with warnings
- **Critical errors**: Pipeline stops with detailed error messages
- **Timeout handling**: Reasonable timeouts for each pipeline stage
- **Retry logic**: Built-in retry for transient failures

## Advanced Usage

### Force Operations
```bash
# Force only raw pipeline
python pipelines/master_pipeline.py --force-raw

# Force only analytics pipeline  
python pipelines/master_pipeline.py --force-analytics

# Force both pipelines
python pipelines/master_pipeline.py --force-all
```

### Status Monitoring
```bash
# System-wide status
python pipelines/master_pipeline.py --status

# Individual pipeline status
python pipelines/raw_pipeline.py --status
python pipelines/analytics_pipeline.py --status
```

### Development Workflow
1. **Test individual components**: Run validation frequently
2. **Check status before running**: Use `--status` flags
3. **Use dry-run for planning**: Preview operations with `--dry-run`
4. **Monitor logs**: Check timestamped logs for detailed progress
5. **Validate after changes**: Always run validation after modifications

## Data Science Integration

### Accessing Data
```python
import duckdb

# Connect to analytics database
conn = duckdb.connect('data/premierleague_analytics.duckdb')

# Query current players
current_players = conn.execute("""
    SELECT player_name, squad, position, goals, assists, touches
    FROM analytics_players 
    WHERE is_current = true
    ORDER BY goals DESC
""").fetchdf()

# Query historical trends
player_progression = conn.execute("""
    SELECT player_name, gameweek, goals, assists, squad
    FROM analytics_players 
    WHERE player_name = 'Erling Haaland'
    ORDER BY gameweek
""").fetchdf()
```

### Example Analyses
- **Player performance trends** across gameweeks
- **Transfer impact analysis** using SCD Type 2 data
- **Team tactical comparisons** using consolidated stats
- **Goalkeeper performance analysis** with specialized metrics

## Troubleshooting

### Common Issues
1. **Import errors after folder restructure**: Clear Python cache with `find . -name "*.pyc" -delete`
2. **Database connection issues**: Check paths in `config/database.yaml`
3. **Scraping failures**: Check internet connection and FBRef availability
4. **SCD validation failures**: Run analytics pipeline with `--force` flag

### Performance Tips
- **Use master pipeline**: More efficient than running individual pipelines
- **Monitor log file sizes**: Logs rotate automatically but check disk space
- **Regular validation**: Run validation weekly to catch issues early
- **Database optimization**: Analytics database automatically optimizes queries

---

**For detailed technical implementation, see the main README.md**