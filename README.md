# Premier League Analytics Platform

A production-ready data pipeline and analytics system for Premier League statistics, featuring intelligent scraping, unified data consolidation, and comprehensive player/team analysis with full historical tracking.

## System Architecture

### Two-Database Design
- **Raw Database** (`premierleague_raw.duckdb`): Preserves original FBRef structure across 33 stat tables
- **Analytics Database** (`premierleague_analytics.duckdb`): Unified analytics with SCD Type 2 historical tracking

### Key Features
- **Unified Data Processing**: Single system handles players, teams, and opponents
- **SCD Type 2 Implementation**: Complete historical tracking with automatic transfer detection
- **Intelligent Pipeline**: Smart decision-making to avoid unnecessary FBRef requests
- **100% Data Coverage**: Processes all scraped data (not just players)
- **Production Logging**: Timestamped, rotated logs with comprehensive validation
- **Entity-Aware Processing**: Consistent handling across all data types

### Fixture-Based Gameweek Tracking
- **Team-Specific Gameweeks**: Each team's gameweek calculated from their completed fixtures  
- **Postponement Handling**: System gracefully handles postponed matches and teams at different gameweeks  
- **Mid-Gameweek Scraping**: Accurate data even when scraped during an ongoing gameweek  
- **Intelligent Updates**: Only processes teams with new data, skipping unchanged teams  

**Example Scenario**: If Manchester City postpones their GW6 match, the system correctly assigns:
- Man City players: GW5 (5 matches played)
- Other teams: GW6 (6 matches played)

This replaces the old single global gameweek approach that couldn't handle real-world complexity.

## Quick Start

### Prerequisites
```bash
pip install duckdb pandas beautifulsoup4 requests pyyaml numpy streamlit plotly
```

### Basic Usage
```bash
# Run complete pipeline (recommended)
python pipelines/master_pipeline.py

# Check system status
python pipelines/master_pipeline.py --status

# Validate entire system
python scripts/validate_analytics_system.py
```

## Pipeline Commands

### Master Pipeline (Recommended)
```bash
python pipelines/master_pipeline.py                    # Smart run (only if needed)
python pipelines/master_pipeline.py --force-all        # Force both pipelines
python pipelines/master_pipeline.py --force-raw        # Force raw only
python pipelines/master_pipeline.py --force-analytics  # Force analytics only
python pipelines/master_pipeline.py --status           # System health check
python pipelines/master_pipeline.py --dry-run          # Preview actions
```

### Individual Pipelines
```bash
# Raw data scraping
python pipelines/raw_pipeline.py                       # Normal run
python pipelines/raw_pipeline.py --status              # Check raw status
python pipelines/raw_pipeline.py --force               # Force refresh

# Analytics ETL
python pipelines/analytics_pipeline.py                 # Normal run  
python pipelines/analytics_pipeline.py --force         # Force refresh
python pipelines/analytics_pipeline.py --status        # Check analytics status
python pipelines/analytics_pipeline.py --validate      # Run validation only
```

### System Validation & Utilities
```bash
# Comprehensive system validation
python scripts/validate_analytics_system.py

# Database creation and management
python scripts/create_analytics_db.py
python scripts/create_analytics_fixtures.py
```

### Applications
```bash
# Team comparison dashboard
streamlit run apps/app_teams.py

# League overview dashboard  
streamlit run apps/app_overview.py
```

### Historical Data Management
```bash
# Load historical seasons
python historical/load_historical_data.py

# Test historical loading
python historical/historical_load_test.py
```

## Project Structure

```
PremierLeagueStatistics/
├── apps/                               # User-facing applications
│   ├── app_overview.py                 # League overview dashboard
│   └── app_teams.py                    # Team comparison tool
├── historical/                         # Historical data management
│   ├── load_historical_data.py         # Historical season loader
│   ├── historical_load_test.py         # Historical loading tests
│   └── HISTORICAL_LOADING.md           # Historical loading guide
├── scripts/                            # Utility scripts
│   ├── validate_analytics_system.py    # System validation
│   ├── create_analytics_db.py          # Database creation
│   └── create_analytics_fixtures.py    # Fixtures setup
├── tests/                              # Test suite
│   └── test.py                         # Test utilities
├── pipelines/                          # Production pipelines
│   ├── master_pipeline.py              # Intelligent orchestration
│   ├── raw_pipeline.py                 # FBRef scraping pipeline
│   └── analytics_pipeline.py           # Unified analytics ETL
├── src/                                # Core library code
│   ├── analytics/                      # Unified analytics components
│   │   ├── analytics_etl.py            # Main ETL engine
│   │   ├── data_consolidation.py       # Unified data consolidator
│   │   ├── scd_processor.py            # SCD Type 2 processor
│   │   ├── fixtures.py                 # Fixtures processing
│   │   └── column_mappings.py          # FBRef → Analytics mapping
│   ├── database/                       # Database connections & operations
│   │   ├── raw_db/                     # Raw database operations
│   │   │   ├── __init__.py
│   │   │   ├── connection.py
│   │   │   └── operations.py
│   │   └── analytics_db/               # Analytics database operations
│   │       ├── __init__.py
│   │       ├── connection.py
│   │       └── operations.py
│   └── scraping/                       # FBRef scraping components
│       ├── __init__.py
│       └── fbref_scraper.py
├── config/                             # Configuration files
│   ├── sources.yaml                    # FBRef URL mappings
│   ├── scraping.yaml                   # Rate limiting settings
│   ├── database.yaml                   # Database configuration
│   └── pipeline.yaml                   # Pipeline settings
├── data/                               # Data storage
│   ├── historical/                     # Historical data
│   │   └── premierleague_raw_historical.duckdb
│   ├── backups/                        # Database backups
│   │   ├── premierleague_analytics_backup_*.duckdb
│   │   └── premierleague_raw_backup_*.duckdb
│   ├── test_data/                      # Test HTML files
│   │   └── test_htmls/
│   ├── logs/                           # Timestamped pipeline logs
│   ├── premierleague_raw.duckdb        # Raw FBRef data (33 tables)
│   └── premierleague_analytics.duckdb  # Unified analytics with SCD Type 2
├── docs/                               # Documentation
│   ├── database_schema.txt
│   ├── *_stats_reference.txt           # Stats documentation
│   └── column_data_dump.md
├── notebooks/                          # Data science notebooks
│   ├── analytics_exploration.ipynb
│   └── top6_analysis.ipynb
├── PIPELINE_USAGE.md                   # Detailed pipeline usage
├── README.md                           # This file
└── requirements.txt                    # Python dependencies
```

### Data Flow
```
FBRef Website → Raw Pipeline → Unified Analytics ETL → Applications
     ↓              ↓                 ↓                      ↓
  Live Data    33 Stat Tables    4 Analytics Tables     ML Models
             Archive Pattern     SCD Type 2 Tracking    Dashboards
             Rate Limited       100% Data Coverage      Notebooks
```

## Data Architecture Details

### Raw Database (33 Tables)
**Squad Tables (11)**: `squad_standard`, `squad_shooting`, `squad_passing`, etc.  
**Opponent Tables (11)**: `opponent_standard`, `opponent_shooting`, etc.  
**Player Tables (11)**: `player_standard`, `player_shooting`, etc.  
**Infrastructure Tables**: `raw_fixtures`, `teams`, `data_scraping_log`

**Note**: Raw data is stored WITHOUT gameweek metadata. Gameweek assignment happens in the analytics layer based on each team's completed fixtures from the `raw_fixtures` table.

### Analytics Database (4 Core Tables)
**`analytics_players`**: Unified outfield player statistics with SCD Type 2  
**`analytics_keepers`**: Goalkeeper-specific metrics with historical tracking  
**`analytics_squads`**: Team performance metrics with SCD Type 2  
**`analytics_opponents`**: Opponent performance when facing each team  
**`analytics_fixtures`**: Match results and metadata

## How Gameweek Tracking Works

### Fixture-Based Approach
The system determines each team's current gameweek by analyzing completed fixtures:

1. **Raw Layer**: Scrapes fixtures with `is_completed` flag
2. **Analytics Layer**: Calculates `MAX(gameweek)` per team where `is_completed = true`
3. **Assignment**: Each player/squad record gets their team's gameweek
4. **SCD Processing**: Updates only teams with new completed fixtures

**Why This Matters**:
- Traditional systems use ONE gameweek for ALL teams (breaks with postponements)
- Fixture-based tracking handles real-world complexity:
  - Postponed matches
  - Mid-gameweek scraping
  - Teams playing at different paces
  - Rescheduled fixtures

**Example**:
```
Fixtures Table:
  GW5: Man City vs Burnley → is_completed = false (postponed)
  GW5: Arsenal vs Liverpool → is_completed = true

Result:
  Man City: gameweek = 4 (last completed fixture)
  Burnley: gameweek = 4
  Arsenal: gameweek = 5
  Liverpool: gameweek = 5
```

This ensures data accuracy even in complex scheduling scenarios.

## Configuration Management

### Core Configuration Files

#### `config/sources.yaml`
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

#### `config/database.yaml`
Database paths and settings:
```yaml
database:
  paths:
    raw: "data/premierleague_raw.duckdb"
    analytics: "data/premierleague_analytics.duckdb"
  connection:
    memory_limit: "2GB"
    threads: 4
```

#### `config/scraping.yaml`
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

## Key Features

### Data Engineering Excellence
- **Complete Data Coverage**: 100% of scraped data processed (vs. 33% in previous versions)  
- **Unified Architecture**: Single consolidation system for all entity types  
- **Production Ready**: Comprehensive validation and error handling  
- **Historical Tracking**: SCD Type 2 implementation across all entities  
- **Performance Optimized**: Processes 400+ entities in ~1 second  

### Ready for Data Science
- **Machine Learning Foundation**: Rich feature set across players, teams, and opponents  
- **Historical Context**: Multi-gameweek tracking for trend analysis  
- **Clean Data**: Validated, consolidated statistics ready for modeling  
- **Scalable Architecture**: Designed for advanced analytics and automation  

### Pipeline Intelligence
The master pipeline uses smart decision-making with team-by-team gameweek tracking:
- **Team-Specific Detection**: Compares gameweeks per team, not globally
- **Incremental Processing**: Only updates teams with new fixtures completed
- **Postponement Awareness**: Handles teams at different gameweeks seamlessly
- **Dependency Tracking**: Analytics runs only after raw data updates
- **Error Recovery**: Comprehensive retry logic and graceful failures

**Example Decision Logic**:
```
Raw Data: 18 teams at GW6, 2 teams at GW5 (postponement)
Analytics: All teams at GW5
Decision: Update only the 18 teams that completed GW6
Result: Mixed gameweeks in analytics (18 teams GW6, 2 teams GW5)
```

## Development Workflow

### Adding New Features
1. **Add new stat categories**: Update `config/sources.yaml`
2. **Extend column mappings**: Modify `src/analytics/column_mappings.py`
3. **Test changes**: Run `python scripts/validate_analytics_system.py`

### Troubleshooting
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

### Common Issues
1. **Import errors after updates**: Clear Python cache with `find . -name "*.pyc" -delete`
2. **Database connection issues**: Check paths in `config/database.yaml`
3. **Scraping failures**: Check internet connection and FBRef availability
4. **SCD validation failures**: Run analytics pipeline with `--force` flag
5. **Missing analytics tables**: Run `python scripts/create_analytics_db.py` to rebuild

### Performance Tips
- **Use master pipeline**: More efficient than running individual pipelines
- **Monitor log file sizes**: Logs rotate automatically but check disk space
- **Regular validation**: Run validation weekly to catch data quality issues early
- **Database optimization**: Analytics database automatically optimizes queries with proper indexing

## Historical Data Management

### Loading Historical Seasons
```bash
# Load historical seasons (see historical/HISTORICAL_LOADING.md for details)
python historical/load_historical_data.py

# Test historical loading functionality
python historical/historical_load_test.py
```

Historical data is stored separately in `data/historical/` and can be loaded into the main analytics database with proper season tagging and historical status marking.

## Analytics Applications

### Team Comparison Dashboard
- **Features**: Side-by-side team analysis across all statistical categories
- **Usage**: `streamlit run apps/app_teams.py`
- **Data**: Real-time analytics from consolidated database

### League Overview Dashboard  
- **Features**: Current standings, top scorers, recent results, upcoming fixtures
- **Usage**: `streamlit run apps/app_overview.py`
- **Data**: Live Premier League statistics and form analysis

## Additional Resources

- **`PIPELINE_USAGE.md`**: Detailed pipeline usage with examples
- **`historical/HISTORICAL_LOADING.md`**: Historical data loading guide
- **`scripts/validate_analytics_system.py`**: Comprehensive system validation
- **`config/`**: Configuration files with inline documentation
- **`src/analytics/column_mappings.py`**: Complete FBRef column mapping reference
- **`notebooks/`**: Data science and analysis examples

## Built With

- **Python 3.12+**: Core language
- **DuckDB**: High-performance analytics database
- **Beautiful Soup**: Web scraping
- **Pandas**: Data manipulation
- **Streamlit**: Interactive dashboards
- **PyYAML**: Configuration management

## Data Source

**FBRef.com** (used respectfully with rate limiting)  
**Architecture**: Two-database system with unified analytics and SCD Type 2 tracking