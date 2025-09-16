# Premier League Analytics Platform

A production-ready data pipeline and analytics system for Premier League statistics, featuring intelligent scraping, historical tracking, and comprehensive player/team analysis.

## 🏗️ Architecture

### Two-Database System
- **Raw Database** (`premierleague_raw.duckdb`): Preserves original FBRef structure across 33 tables
- **Analytics Database** (`premierleague_analytics.duckdb`): SCD Type 2 with 144 columns and derived metrics

### Key Features
- **SCD Type 2 Implementation**: Complete historical tracking with transfer detection
- **Intelligent Pipeline**: Smart decision-making to avoid unnecessary FBRef requests  
- **Derived Metrics**: 15 advanced analytics including form scores and efficiency ratings
- **Production Logging**: Timestamped, rotated logs with granular progress tracking

## 🚀 Quick Start

### Prerequisites
```bash
pip install duckdb pandas beautifulsoup4 requests pyyaml numpy
```

### Basic Usage
```bash
# Run complete pipeline (recommended)
python pipelines/master_pipeline.py

# Check system status
python pipelines/master_pipeline.py --status

# Validate data quality
python validate_analytics_system.py
```

## 📊 Pipeline Commands

### Master Pipeline (Orchestration)
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

## 🏛️ System Architecture

### Project Structure
```
PremierLeagueStatistics/
├── pipelines/                          # Production pipeline scripts
│   ├── master_pipeline.py              # Intelligent orchestration
│   ├── raw_pipeline.py                 # FBRef scraping
│   └── analytics_pipeline.py           # SCD Type 2 ETL
├── src/                                # Core library
│   ├── analytics/etl/                  # Analytics components
│   │   ├── analytics_etl.py            # Main ETL engine
│   │   ├── player_consolidation.py     # Data consolidation
│   │   └── derived_metrics.py          # Metrics calculation
│   ├── database/                       # Database layer
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
│   ├── premierleague_raw.duckdb        # Raw FBRef data
│   └── premierleague_analytics.duckdb  # Analytics with SCD Type 2
├── notebooks/                          # Data science notebooks
├── validate_analytics_system.py        # Comprehensive system validation
└── PIPELINE_USAGE.md                   # Detailed usage guide
```

### Data Flow
```
FBRef Website → Raw Pipeline → Analytics ETL → Applications
     ↓              ↓              ↓              ↓
  Live Data    33 Stat Tables  SCD Type 2    ML Models
                Archive Pattern  144 Columns  Dashboards
                Rate Limited    15 Metrics    Notebooks
```

## 📈 Data Architecture Details

### Raw Database (33 Tables)
**Squad Tables (11)**: `squad_standard`, `squad_shooting`, `squad_passing`, etc.  
**Opponent Tables (11)**: `opponent_standard`, `opponent_shooting`, etc.  
**Player Tables (11)**: `player_standard`, `player_shooting`, etc.  
**Infrastructure**: `raw_fixtures`, `teams`, `data_scraping_log`

### Analytics Database (SCD Type 2)
**Core Table**: `analytics_players` (144 columns)
- **Dimensions**: player_name, squad, position, nation, age
- **Time Tracking**: gameweek, valid_from, valid_to, is_current
- **Stats**: 127 consolidated statistics from all 11 FBRef categories
- **Derived Metrics**: 15 calculated analytics (goals_vs_expected, form_score, etc.)

### SCD Type 2 Example
```sql
-- Eze's transfer tracking
player_key | player_name | squad   | gameweek | is_current | goals | assists
----------|-------------|---------|----------|------------|-------|--------
1001      | Eze         | Palace  | 4        | false      | 0     | 0      (Historical)
1002      | Eze         | Arsenal | 5        | true       | 0     | 1      (Current)
```

## 🔧 Configuration

### Key Configuration Files

**`config/sources.yaml`**: FBRef URL mappings
```yaml
stats_sources:
  standard:
    url: "https://fbref.com/en/comps/9/stats/Premier-League-Stats"
    tables: ["squad_standard", "opponent_standard", "player_standard"]
```

**`config/scraping.yaml`**: Rate limiting and scraping behavior
```yaml
scraping:
  delays:
    between_requests: 10  # Respectful to FBRef
```

**`config/database.yaml`**: Database paths and settings
```yaml
database:
  paths:
    raw: "data/premierleague_raw.duckdb"
    analytics: "data/premierleague_analytics.duckdb"
```

## 🧪 Data Quality & Validation

### Automated Validation
```bash
python validate_analytics_system.py
```

**Validates**:
- SCD Type 2 integrity (only current gameweek marked as current)
- Player tracking across gameweeks and transfers
- Derived metrics calculation and coverage
- Data quality checks (duplicates, missing data, logical consistency)
- System health and performance metrics

### Manual Data Exploration
```python
import duckdb

# Connect to analytics database
conn = duckdb.connect('data/premierleague_analytics.duckdb')

# View current players
current_players = conn.execute("""
    SELECT player_name, squad, goals, assists, minutes_played 
    FROM analytics_players 
    WHERE is_current = true 
    ORDER BY goals DESC 
    LIMIT 10
""").fetchdf()

# Track player history
player_history = conn.execute("""
    SELECT gameweek, squad, goals, assists, is_current
    FROM analytics_players 
    WHERE player_name = 'Erling Haaland'
    ORDER BY gameweek
""").fetchdf()
```

## 🔍 System Monitoring

### Pipeline Intelligence
The master pipeline automatically:
- Checks local gameweek status
- Makes minimal FBRef requests only when needed
- Skips unnecessary work when data is current
- Handles analytics-only updates when raw is current

### Logging
- **Timestamped logs**: `data/logs/master_pipeline_20250915_213045.log`
- **Automatic rotation**: Keeps last 5 logs
- **Granular tracking**: Progress through scraping categories, ETL steps
- **Error handling**: Comprehensive error capture and reporting

### Health Checks
```bash
# System status overview
python pipelines/master_pipeline.py --status

# Detailed validation
python validate_analytics_system.py

# Individual component status  
python pipelines/raw_pipeline.py --status
python pipelines/analytics_pipeline.py --status
```

## 🚦 System Status

### Current State (as of latest run)
- **Raw Database**: Gameweek 5, 404 players across 33 tables
- **Analytics Database**: Gameweek 5, 774 total records (370 historical + 404 current)
- **SCD Type 2**: Working perfectly with transfer detection
- **Derived Metrics**: 15 metrics calculated with 100% coverage
- **Data Quality**: All validation checks passing

### Transfer Tracking Example
System successfully tracks mid-gameweek transfers:
- **Eberechi Eze**: Crystal Palace → Arsenal (tracked in both GW4 and GW5)
- **Harvey Elliott**: Liverpool ↔ Aston Villa (loan tracking)

## 🔮 Ready for Next Phase

### Current Capabilities
- ✅ Production-ready data pipeline
- ✅ Complete historical tracking with SCD Type 2
- ✅ 144-column analytics with derived metrics
- ✅ Transfer impact analysis capabilities
- ✅ Comprehensive data validation
- ✅ Intelligent pipeline orchestration

### Ready for Machine Learning
- **774 training examples** and growing each gameweek
- **144 features** per player per gameweek
- **Complete historical context** for time series modeling
- **Transfer impact data** for specialized ML models
- **Data quality assured** through comprehensive validation

### Potential Next Steps
1. **Data Science Notebooks**: Player analysis, team comparisons, trend identification
2. **Machine Learning Models**: Performance prediction, transfer value assessment
3. **Advanced Analytics**: Tactical analysis, formation effectiveness, player development
4. **Automation**: Scheduled runs, monitoring alerts, backup automation

## 🛠️ Development

### Adding New Derived Metrics
1. Edit `src/analytics/etl/derived_metrics.py`
2. Add calculation logic to `calculate_derived_metrics()`
3. Update analytics schema if needed
4. Test with `python validate_analytics_system.py`

### Extending Scraping
1. Add new stat category to `config/sources.yaml`
2. Raw pipeline automatically includes new categories
3. Update consolidation logic in `player_consolidation.py` if needed

### Database Queries
```python
# Example: Find top goal scorers by gameweek
top_scorers = conn.execute("""
    SELECT gameweek, player_name, squad, goals,
           RANK() OVER (PARTITION BY gameweek ORDER BY goals DESC) as rank
    FROM analytics_players 
    WHERE goals > 0
    QUALIFY rank <= 5
    ORDER BY gameweek, rank
""").fetchdf()
```

## 📚 Additional Resources

- **`PIPELINE_USAGE.md`**: Detailed pipeline usage examples
- **`validate_analytics_system.py`**: Comprehensive system validation
- **`config/`**: All configuration files with inline documentation
- **`notebooks/`**: Data science and analysis examples (to be created)

---

**Built with**: Python, DuckDB, Beautiful Soup, Pandas  
**Data Source**: FBRef.com (used respectfully with rate limiting)  
**Architecture**: Two-database system with SCD Type 2 historical tracking