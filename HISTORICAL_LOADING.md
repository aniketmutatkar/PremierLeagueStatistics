# Historical Data Loading Guide

**One-Time Historical Season Loading for Premier League Analytics System**

This guide documents the process for loading additional historical seasons into your analytics database. This is a **one-time operation** per season and should only be needed when adding new historical data.

---

## Overview

The historical loader scrapes complete season data from FBRef and processes it through your existing analytics pipeline with modifications to ensure proper season handling and historical status.

**Architecture:** 
FBRef → Raw Historical DB → Analytics Pipeline → Analytics DB (marked as historical)

---

## Prerequisites

1. Production system should be working normally
2. You should have a clean backup of your current analytics database
3. Ensure you have stable internet connection (process takes ~15 minutes per season)

---

## Step-by-Step Process

### 1. Backup Your Current Database

```bash
# Backup your current analytics database
cp data/premierleague_analytics.duckdb data/premierleague_analytics_backup_$(date +%Y%m%d).duckdb
```

### 2. Update Database Configuration

**Edit `config/database.yaml`:**

```yaml
paths:
  raw: "data/premierleague_raw_historical.duckdb"                 # Temporary historical raw DB
  analytics: "data/premierleague_analytics_historytest.duckdb"   # Test database for loading
```

### 3. Add Temporary Environment Variable Support

**In `src/analytics/data_consolidation.py`** (in `_add_scd_metadata` method):

```python
# Add this temporary code for historical loading:
import os
historical_season = os.getenv('HISTORICAL_SEASON')
if historical_season:
    df['season'] = historical_season
else:
    scraper = FBRefScraper()
    df['season'] = scraper._extract_season_info()
```

**In `src/analytics/scd_processor.py`** (in both `_prepare_scd_records` and `_prepare_entity_scd_records` methods):

```python
# Add this temporary code for historical loading:
import os
historical_season = os.getenv('HISTORICAL_SEASON')
if historical_season:
    scd_data['season'] = historical_season
else:
    scraper = FBRefScraper()
    scd_data['season'] = scraper._extract_season_info()
```

### 4. Update Historical Loader Configuration

**In `load_historical_data.py`**, ensure `HistoricalAnalyticsProcessor` uses correct database:

```python
class HistoricalAnalyticsProcessor:
    def __init__(self):
        self.historical_raw_db_path = "data/premierleague_raw_historical.duckdb"
        self.test_analytics_db_path = "data/premierleague_analytics_historytest.duckdb"  # Make sure this matches config
```

### 5. Configure Seasons to Load

**In `load_historical_data.py`**, update the seasons list in `main()`:

```python
def main():
    # Define seasons to load - ADD NEW SEASONS HERE
    historical_seasons = ["2017-2018", "2016-2017"]  # Example: adding older seasons
    
    # ... rest of function
```

### 6. Run Historical Loader

```bash
python3 load_historical_data.py
```

**Expected output:**
```
SEASON 1/2: 2017-2018
... (scraping and processing)
✅ SUCCESS: 2017-2018 loaded

SEASON 2/2: 2016-2017
... (scraping and processing)
✅ SUCCESS: 2016-2017 loaded

🎉 ALL 2 SEASONS LOADED SUCCESSFULLY!
```

### 7. Validate Results

```bash
python3 test.py
```

**Expected validation results:**
- All new seasons present with `is_current = false`
- Business keys include season information
- No duplicate records
- Reasonable player counts per season

### 8. Integrate with Production

**Option A: Replace Production Database**
```bash
# Backup current production
mv data/premierleague_analytics.duckdb data/premierleague_analytics_old.duckdb

# Use loaded database as production
mv data/premierleague_analytics_historytest.duckdb data/premierleague_analytics.duckdb
```

**Option B: Copy Historical Data**
```sql
-- Use SQL to copy only historical records from test DB to production DB
ATTACH 'data/premierleague_analytics_historytest.duckdb' AS test_db;
INSERT INTO analytics_players SELECT * FROM test_db.analytics_players WHERE season IN ('2017-2018', '2016-2017');
-- Repeat for other tables...
```

### 9. Cleanup and Restore

**Update `config/database.yaml` back to production:**

```yaml
paths:
  raw: "data/premierleague_raw.duckdb"                    # Back to production
  analytics: "data/premierleague_analytics.duckdb"       # Back to production
```

**Remove temporary environment variable code** from:
- `src/analytics/data_consolidation.py`
- `src/analytics/scd_processor.py`

**Restore original code:**
```python
scraper = FBRefScraper()
df['season'] = scraper._extract_season_info()
```

### 10. Test Production Pipeline

```bash
# Verify production pipeline still works
python3 pipelines/master_pipeline.py --status
python3 pipelines/master_pipeline.py
```

---

## Important Notes

### Season Format
- Use format: `"YYYY-YYYY"` (e.g., `"2017-2018"`)
- Premier League seasons run August to May
- Historical seasons should be complete (ended) seasons only

### Rate Limiting
- Script includes 10-second delays between requests to be respectful to FBRef
- Total time: ~15 minutes per season
- Don't modify delays to avoid being blocked

### Business Key Format
Historical loading ensures business keys include season:
- Players: `"PlayerName_BirthYear_Squad_Season"`
- Squads: `"SquadName_Season"`

### Data Validation
Always run `test.py` after loading to verify:
- Correct season assignment
- Historical status (`is_current = false`)
- No duplicate records
- Reasonable data quality

### Troubleshooting

**Primary Key Conflicts:**
- Ensure you're using a clean test database
- Check that environment variable code is properly added

**Wrong Season Assignment:**
- Verify environment variable is being set and read correctly
- Check that seasonal override is working in scraper

**Missing Data:**
- Some older seasons may have fewer available statistics
- Check FBRef manually to verify data availability

---

## Clean Up After Loading

1. Delete temporary files: `premierleague_raw_historical.duckdb`
2. Remove `load_historical_data.py` and `test.py` if no longer needed
3. Document which seasons are now available in your main README
4. Update any analysis notebooks to include historical data ranges

---

## Future Considerations

This is a bandaid solution for one-time historical loading. If you need to regularly load historical data, consider:

1. Refactoring analytics pipeline to accept season parameters
2. Creating a more robust historical data management system
3. Implementing automated historical data updates

For now, this approach works well for the occasional addition of historical seasons to your analytics database.