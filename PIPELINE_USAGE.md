
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
│   ├── analytics/             # Analytics ETL components
│   ├── database/              # Database connections & operations
│   └── scraping/              # FBRef scraping components
├── config/                    # Configuration files
├── data/                      # Data storage
│   ├── logs/                  # Pipeline logs
│   └── backups/               # Database backups
├── notebooks/                 # Data science notebooks
└── docs/                      # Documentation
```

## Data Architecture

### Two-Database System
- **Raw Database** (`premierleague_raw.duckdb`): Preserves original FBRef structure
- **Analytics Database** (`premierleague_analytics.duckdb`): SCD Type 2 with 144 columns

### SCD Type 2 Implementation
- Complete historical tracking of all players
- Transfer detection and impact analysis  
- Performance progression over time
- 774 records tracking gameweeks 4-5 with full history
