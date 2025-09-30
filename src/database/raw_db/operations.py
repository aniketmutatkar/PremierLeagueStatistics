"""
Raw Database Operations - NEW FIXTURE-BASED IMPLEMENTATION
Phase 2: Remove gameweek tagging from raw data
"""
import pandas as pd
import logging
from datetime import datetime
from typing import Dict, Any, List
from .connection import RawDatabaseConnection

logger = logging.getLogger(__name__)

class RawDatabaseOperations:
    """NEW: Raw database operations without gameweek tagging"""
    
    def __init__(self, db_connection: RawDatabaseConnection):
        self.db = db_connection
        logger.info("Raw database operations initialized (NEW fixture-based mode)")
    
    def create_infrastructure_tables(self):
        """Create infrastructure tables (fixtures, teams)"""
        logger.info("Creating infrastructure tables...")
        
        with self.db.get_connection() as conn:
            self._create_fixtures_table(conn)
            self._create_teams_table(conn)
        
        logger.info("Infrastructure tables created successfully")
    
    def _create_fixtures_table(self, conn):
        """Create fixtures table (UNCHANGED - fixtures table stays the same)"""
        sql = """
        CREATE TABLE IF NOT EXISTS raw_fixtures (
            fixture_id VARCHAR PRIMARY KEY,
            gameweek INTEGER,
            match_date DATE,
            match_time TIME,
            home_team VARCHAR,
            away_team VARCHAR,
            home_score INTEGER,
            away_score INTEGER,
            home_xg FLOAT,
            away_xg FLOAT,
            venue VARCHAR,
            referee VARCHAR,
            attendance INTEGER,
            is_completed BOOLEAN DEFAULT FALSE,
            scraped_date DATE DEFAULT CURRENT_DATE
        )
        """
        conn.execute(sql)
        logger.debug("Created raw_fixtures table")
    
    def _create_teams_table(self, conn):
        """Create teams table (UNCHANGED)"""
        sql = """
        CREATE TABLE IF NOT EXISTS teams (
            team_name VARCHAR PRIMARY KEY,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        conn.execute(sql)
        logger.debug("Created teams table")
    
    def insert_clean_stat_table(self, table_name: str, data: pd.DataFrame):
        """
        NEW: Insert stat table WITHOUT gameweek tagging
        
        Args:
            table_name: e.g., 'squad_standard', 'player_passing', etc.
            data: Clean DataFrame ready for insertion
            
        REMOVED: current_gameweek parameter - no longer tags data with gameweek
        """
        if data.empty:
            logger.warning(f"No data to insert for {table_name}")
            return
        
        # NEW: Only add last_updated, NO gameweek tagging
        data = data.copy()
        data['last_updated'] = datetime.now().date()
        
        with self.db.get_connection() as conn:
            try:
                # Drop and recreate table (overwrite approach)
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                
                # Create new table with current data structure
                conn.register('temp_raw_data', data)
                conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_raw_data")
                conn.unregister('temp_raw_data')
                
                logger.info(f"Inserted {len(data)} rows into {table_name}")
                
            except Exception as e:
                logger.error(f"Failed to insert data into {table_name}: {e}")
                raise
    
    def insert_fixtures(self, fixtures_df: pd.DataFrame):
        """
        NEW: Insert fixture data WITHOUT gameweek parameter
        
        Args:
            fixtures_df: DataFrame with fixture data
            
        REMOVED: current_gameweek parameter
        """
        self.insert_clean_stat_table('raw_fixtures', fixtures_df)
    
    def get_raw_database_status(self) -> Dict[str, Any]:
        """Get status of raw database"""
        with self.db.get_connection() as conn:
            try:
                # Get all table names
                tables_result = conn.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema='main' AND table_type='BASE TABLE'
                """).fetchall()
                
                all_tables = [row[0] for row in tables_result]
                
                # Categorize tables
                stat_tables = [t for t in all_tables if any(t.startswith(prefix) for prefix in ['squad_', 'opponent_', 'player_'])]
                infrastructure_tables = [t for t in all_tables if t not in stat_tables]
                
                status = {
                    'total_tables': len(all_tables),
                    'stat_tables': len(stat_tables),
                    'infrastructure_tables': len(infrastructure_tables),
                    'table_names': all_tables,
                    'table_details': {}
                }
                
                # Get row counts for each table
                for table_name in all_tables:
                    try:
                        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                        status['table_details'][table_name] = {
                            'row_count': row_count
                        }
                    except Exception as e:
                        status['table_details'][table_name] = {'error': str(e)}
                
                return status
                
            except Exception as e:
                logger.error(f"Failed to get raw database status: {e}")
                return {'error': str(e)}
    
    def validate_raw_data_quality(self) -> Dict[str, str]:
        """Validate raw data quality"""
        results = {}
        
        with self.db.get_connection() as conn:
            try:
                # Check for expected stat table categories
                expected_categories = ['standard', 'keepers', 'keepersadv', 'shooting', 'passing', 
                                     'passingtypes', 'goalshotcreation', 'defense', 'possession', 
                                     'playingtime', 'misc']
                
                for category in expected_categories:
                    squad_table = f"squad_{category}"
                    opponent_table = f"opponent_{category}"
                    player_table = f"player_{category}"
                    
                    # Check if all 3 tables exist for this category
                    tables_exist = []
                    for table_name in [squad_table, opponent_table, player_table]:
                        try:
                            conn.execute(f"SELECT COUNT(*) FROM {table_name} LIMIT 1")
                            tables_exist.append(True)
                        except:
                            tables_exist.append(False)
                    
                    if all(tables_exist):
                        results[category] = f"✅ {category}: All 3 tables present"
                    else:
                        missing = [t for t, exists in zip([squad_table, opponent_table, player_table], tables_exist) if not exists]
                        results[category] = f"❌ {category}: Missing tables: {missing}"
                
                # Check fixtures table
                try:
                    fixture_count = conn.execute("SELECT COUNT(*) FROM raw_fixtures").fetchone()[0]
                    completed_count = conn.execute("SELECT COUNT(*) FROM raw_fixtures WHERE is_completed = true").fetchone()[0]
                    results['fixtures'] = f"✅ Fixtures: {fixture_count} total, {completed_count} completed"
                except Exception as e:
                    results['fixtures'] = f"❌ Fixtures: {e}"
                
                return results
                
            except Exception as e:
                logger.error(f"Failed to validate raw data quality: {e}")
                return {'error': str(e)}
    
    def check_stat_category_completion(self, category: str) -> Dict[str, Any]:
        """Check completion status for a stat category"""
        with self.db.get_connection() as conn:
            try:
                squad_table = f"squad_{category}"
                opponent_table = f"opponent_{category}"
                player_table = f"player_{category}"
                
                result = {
                    'category': category,
                    'tables': {},
                    'summary': {}
                }
                
                total_rows = 0
                all_exist = True
                
                for table_name in [squad_table, opponent_table, player_table]:
                    try:
                        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                        result['tables'][table_name] = {
                            'exists': True,
                            'row_count': row_count
                        }
                        total_rows += row_count
                    except Exception as e:
                        result['tables'][table_name] = {
                            'exists': False,
                            'error': str(e)
                        }
                        all_exist = False
                
                result['summary'] = {
                    'all_tables_exist': all_exist,
                    'total_rows': total_rows
                }
                
                return result
                
            except Exception as e:
                logger.error(f"Failed to check {category} completion: {e}")
                return {'error': str(e)}