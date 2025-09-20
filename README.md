# Premier League Analytics Platform

A production-ready data pipeline and analytics system for Premier League statistics, featuring intelligent scraping, unified data consolidation, and comprehensive player/team analysis with full historical tracking.

## 🏗️ System Architecture

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

# Validate entire system
python validate_analytics_system.py
```

## 📊 Pipeline Commands

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

## 🏛️ System Architecture

### Project Structure
```
PremierLeagueStatistics/
├── pipelines/                          # Production pipeline scripts
│   ├── master_pipeline.py              # Intelligent orchestration
│   ├── raw_pipeline.py                 # FBRef scraping
│   └── analytics_pipeline.py           # Unified analytics ETL
├── src/                                # Core library
│   ├── analytics/                      # Analytics components
│   │   ├── analytics_etl.py            # Main ETL engine
│   │   ├── data_consolidation.py       # Unified data consolidator
│   │   ├── scd_processor.py            # SCD Type 2 processor
│   │   └── column_mappings.py          # FBRef → Analytics mapping
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
│   └── premierleague_analytics.duckdb  # Unified analytics with SCD Type 2
├── notebooks/                          # Data science notebooks
├── validate_analytics_system.py        # Comprehensive system validation
└── PIPELINE_USAGE.md                   # Detailed usage guide
```

### Data Flow
```
FBRef Website → Raw Pipeline → Unified Analytics ETL → Applications
     ↓              ↓                 ↓                      ↓
  Live Data    33 Stat Tables    4 Analytics Tables     ML Models
             Archive Pattern     SCD Type 2 Tracking    Dashboards
             Rate Limited       100% Data Coverage      Notebooks
```

## 📈 Data Architecture Details

### Raw Database (33 Tables)
**Squad Tables (11)**: `squad_standard`, `squad_shooting`, `squad_passing`, etc.  
**Opponent Tables (11)**: `opponent_standard`, `opponent_shooting`, etc.  
**Player Tables (11)**: `player_standard`, `player_shooting`, etc.

### Analytics Database (4 Tables) - NEW UNIFIED SYSTEM
**Complete Entity Coverage:**
- **`analytics_players`** (154 columns): Outfield player statistics with SCD Type 2
- **`analytics_keepers`** (64 columns): Goalkeeper-specific metrics with SCD Type 2  
- **`analytics_squads`** (185 columns): Team-level analytics with SCD Type 2
- **`analytics_opponents`** (185 columns): Opposition analysis with SCD Type 2

**Key Features:**
- **100% Data Processing**: All 33 raw tables consolidated into 4 analytics tables
- **Unified Consolidation**: Single `DataConsolidator` handles all entity types
- **Historical Tracking**: Complete SCD Type 2 across all entities
- **Entity-Aware Processing**: Consistent logic for players, teams, and opponents

### Current System Status (Example)
```
📋 SYSTEM SUMMARY:
  analytics_players: 380 current, 349 historical (GW 5)
  analytics_keepers: 24 current, 21 historical (GW 5)  
  analytics_squads: 20 current, 20 historical (GW 5)
  analytics_opponents: 20 current, 20 historical (GW 5)
```

## 🔍 System Validation

### Comprehensive Validation
```bash
python validate_analytics_system.py
```

**Validation Coverage:**
- **Schema Validation**: All 4 analytics tables structure verification
- **SCD Type 2 Integrity**: Historical tracking correctness across all entities
- **Data Quality**: Missing data and consistency checks
- **Cross-Entity Relationships**: Squad/opponent/player relationship validation
- **Business Logic**: Statistical sanity checks and Premier League constraints

## 💡 Usage Examples

### Data Access
```python
import duckdb

# Connect to unified analytics database
conn = duckdb.connect('data/premierleague_analytics.duckdb')

# Query current top scorers
top_scorers = conn.execute("""
    SELECT player_name, squad_name, goals, assists
    FROM analytics_players 
    WHERE is_current = true 
    ORDER BY goals DESC 
    LIMIT 10
""").fetchdf()

# Query team offensive stats
team_offense = conn.execute("""
    SELECT squad_name, goals, shots, expected_goals
    FROM analytics_squads 
    WHERE is_current = true 
    ORDER BY goals DESC
""").fetchdf()

# Historical player progression
player_history = conn.execute("""
    SELECT gameweek, player_name, squad_name, goals, valid_from, valid_to
    FROM analytics_players 
    WHERE player_name = 'Erling Haaland'
    ORDER BY gameweek
""").fetchdf()
```

### Analysis Capabilities
- **Player Analysis**: 404 tracked players with complete performance history
- **Team Analytics**: 20 Premier League squads with tactical and performance data
- **Opposition Scouting**: 20 opponent profiles for strategic analysis
- **Transfer Tracking**: Automatic detection of player movements between teams
- **Performance Trends**: Multi-gameweek analysis across all entity types

## 🛠️ Development

### System Extension
The unified architecture makes extending the system straightforward:

1. **Add new stat categories**: Update `config/sources.yaml`
2. **Extend column mappings**: Modify `src/analytics/column_mappings.py`
3. **Test changes**: Run `python validate_analytics_system.py`

### Pipeline Intelligence
The master pipeline uses smart decision-making:
- **Incremental processing**: Only runs when new data is available
- **Dependency tracking**: Analytics runs only after raw data updates
- **Error recovery**: Comprehensive retry logic and graceful failures

## 🎯 Achievements

### Data Engineering Excellence
✅ **Complete Data Coverage**: 100% of scraped data processed (vs. 33% in previous versions)  
✅ **Unified Architecture**: Single consolidation system for all entity types  
✅ **Production Ready**: Comprehensive validation and error handling  
✅ **Historical Tracking**: SCD Type 2 implementation across all entities  
✅ **Performance Optimized**: Processes 400+ entities in ~1 second  

### Ready for Data Science
✅ **Machine Learning Foundation**: Rich feature set across players, teams, and opponents  
✅ **Historical Context**: Multi-gameweek tracking for trend analysis  
✅ **Clean Data**: Validated, consolidated statistics ready for modeling  
✅ **Scalable Architecture**: Designed for advanced analytics and automation  

## 📚 Additional Resources

- **`PIPELINE_USAGE.md`**: Detailed pipeline usage with examples
- **`validate_analytics_system.py`**: Comprehensive system validation
- **`config/`**: Configuration files with inline documentation
- **`src/analytics/column_mappings.py`**: Complete FBRef column mapping reference
- **`notebooks/`**: Data science and analysis examples

---

**Built with**: Python, DuckDB, Beautiful Soup, Pandas  
**Data Source**: FBRef.com (used respectfully with rate limiting)  
**Architecture**: Two-database system with unified analytics and SCD Type 2 tracking