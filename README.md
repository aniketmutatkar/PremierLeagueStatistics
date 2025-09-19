# Premier League Analytics Platform

A production-ready data pipeline and analytics system for Premier League statistics, featuring intelligent scraping, historical tracking, and comprehensive player/team analysis.

## 🏗️ Architecture

### Two-Database System
- **Raw Database** (`premierleague_raw.duckdb`): Preserves original FBRef structure across 33 tables
- **Analytics Database** (`premierleague_analytics.duckdb`): SCD Type 2 with clean, consolidated statistics

### Key Features
- **SCD Type 2 Implementation**: Complete historical tracking with transfer detection
- **Intelligent Pipeline**: Smart decision-making to avoid unnecessary FBRef requests  
- **Separated Player Types**: Dedicated tables for outfield players and goalkeepers
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
│   │   └── player_consolidation.py     # Data consolidation
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
                Archive Pattern  Clean Stats   Dashboards
                Rate Limited    Two Tables    Notebooks
```

## 📈 Data Architecture Details

### Raw Database (33 Tables)
**Squad Tables (11)**: `squad_standard`, `squad_shooting`, `squad_passing`, etc.  
**Opponent Tables (11)**: `opponent_standard`, `opponent_shooting`, etc.  
**Player Tables (11)**: `player_standard`, `player_shooting`, etc.  
**Infrastructure**: `raw_fixtures`, `teams`, `data_scraping_log`

### Analytics Database (SCD Type 2)
**Core Tables**: 
- **`analytics_players`** (~160 columns): Outfield players (DF, MF, FW) with comprehensive statistics
- **`analytics_keepers`** (~60 columns): Goalkeepers with specialized metrics including advanced shot-stopping data

**Dimensions**: player_name, squad, position, nation, age  
**Time Tracking**: gameweek, valid_from, valid_to, is_current  
**Stats**: Raw FBRef statistics with explicit column mapping

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
- Data consolidation quality and column mapping success
- Data quality checks (duplicates, missing data, logical consistency)
- Statistical sanity checks (players have touches > 0, etc.)

## 🎯 Current Status
- **Raw Pipeline**: ✅ Stable, scrapes all 33 FBRef stat tables
- **Analytics Pipeline**: ✅ Clean consolidation with explicit column mapping
- **Data Quality**: ✅ 160+ columns for outfield players, 60+ for goalkeepers
- **Historical Tracking**: ✅ SCD Type 2 implementation working
- **Transfer Detection**: ✅ Players tracked across team changes

## 🔮 Future Development

1. **Advanced Analytics**: 
   - **Data Science Notebooks**: Player analysis, team comparisons, trend identification
   - **Machine Learning Models**: Performance prediction, transfer value assessment
   - **Advanced Analytics**: Tactical analysis, formation effectiveness, player development
   - **Automation**: Scheduled runs, monitoring alerts, backup automation

2. **Enhanced Features**:
   - Real-time dashboard integration
   - Custom derived metrics (when needed)
   - Multi-season historical analysis
   - Advanced statistical modeling

## 🛠️ Development

### Extending Scraping
1. Add new stat category to `config/sources.yaml`
2. Raw pipeline automatically includes new categories
3. Update consolidation logic in `player_consolidation.py` if needed
4. Test with `python validate_analytics_system.py`

### Database Queries
```python
# Example: Find top goal scorers by gameweek
top_scorers = conn.execute("""
    SELECT gameweek, player_name, squad, goals,
           RANK() OVER (PARTITION BY gameweek ORDER BY goals DESC) as rank
    FROM analytics_players 
    WHERE goals > 0 AND is_current = true
    QUALIFY rank <= 5
    ORDER BY gameweek, rank
""").fetchdf()

# Example: Goalkeeper performance
keeper_stats = conn.execute("""
    SELECT player_name, squad, saves, save_percentage, clean_sheets,
           post_shot_expected_goals, post_shot_xg_performance
    FROM analytics_keepers 
    WHERE is_current = true AND minutes_played > 270
    ORDER BY save_percentage DESC
""").fetchdf()
```

### Schema Management
The analytics database uses explicit column mapping from raw FBRef data:
- **No prefixes**: Clean column names like `touches`, `tackles`, `shots`
- **No derived metrics**: Only raw FBRef statistics (can add calculated metrics later)
- **Type separation**: Outfield players and goalkeepers in separate tables
- **Full coverage**: ~220 out of 327 raw columns mapped (67% utilization)

## 📚 Additional Resources

- **`PIPELINE_USAGE.md`**: Detailed pipeline usage examples
- **`validate_analytics_system.py`**: Comprehensive system validation
- **`config/`**: All configuration files with inline documentation
- **`notebooks/`**: Data science and analysis examples (to be created)

---

**Built with**: Python, DuckDB, Beautiful Soup, Pandas  
**Data Source**: FBRef.com (used respectfully with rate limiting)  
**Architecture**: Two-database system with SCD Type 2 historical tracking