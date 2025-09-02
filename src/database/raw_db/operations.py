"""
Raw Database Operations - Following archive pattern exactly
Handles 33 individual stat tables (11 categories × 3 types)
"""
import pandas as pd
import logging
from datetime import datetime
from typing import Dict, Any, List
from .connection import RawDatabaseConnection

logger = logging.getLogger(__name__)

class RawDatabaseOperations:
    """Database operations for raw data storage - follows archive pattern"""
    
    def __init__(self, db_connection: RawDatabaseConnection):
        self.db = db_connection
        logger.info("Raw database operations initialized")
    
    def create_infrastructure_tables(self):
        """Create infrastructure tables (fixtures, teams)"""
        logger.info("Creating infrastructure tables...")
        
        with self.db.get_connection() as conn:
            self._create_fixtures_table(conn)
            self._create_teams_table(conn)
        
        logger.info("Infrastructure tables created successfully")
    
    def _create_fixtures_table(self, conn):
        """Create fixtures table"""
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
        """Create teams table"""
        sql = """
        CREATE TABLE IF NOT EXISTS teams (
            team_name VARCHAR PRIMARY KEY,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        conn.execute(sql)
        logger.debug("Created teams table")
    
    def insert_clean_stat_table(self, table_name: str, data: pd.DataFrame, current_gameweek: int):
        """
        Insert a single clean stat table (following archive pattern)
        
        Args:
            table_name: e.g., 'squad_standard', 'player_passing', etc.
            data: Clean DataFrame ready for insertion
            current_gameweek: Current gameweek number
        """
        if data.empty:
            logger.warning(f"No data to insert for {table_name}")
            return
        
        # Add metadata (following archive pattern)
        data = data.copy()
        data['current_through_gameweek'] = current_gameweek
        data['last_updated'] = datetime.now().date()
        
        with self.db.get_connection() as conn:
            try:
                # Drop and recreate table (overwrite approach like archive)
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                
                # Create new table with current data structure
                conn.register('temp_raw_data', data)
                conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_raw_data")
                conn.unregister('temp_raw_data')
                
                logger.info(f"Inserted {len(data)} rows into {table_name} for gameweek {current_gameweek}")
                
            except Exception as e:
                logger.error(f"Failed to insert data into {table_name}: {e}")
                raise
    
    def insert_fixtures(self, fixtures_df: pd.DataFrame, current_gameweek: int):
        """Insert fixture data"""
        self.insert_clean_stat_table('raw_fixtures', fixtures_df, current_gameweek)
    
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
                
                table_names = [row[0] for row in tables_result]
                
                # Categorize tables
                stat_tables = []
                infrastructure_tables = []
                
                for table_name in table_names:
                    if any(table_name.startswith(prefix) for prefix in ['squad_', 'opponent_', 'player_']):
                        stat_tables.append(table_name)
                    else:
                        infrastructure_tables.append(table_name)
                
                status = {
                    'total_tables': len(table_names),
                    'stat_tables': len(stat_tables),
                    'infrastructure_tables': len(infrastructure_tables),
                    'stat_table_names': sorted(stat_tables),
                    'infrastructure_table_names': sorted(infrastructure_tables),
                    'table_details': {}
                }
                
                # Get details for each table
                for table_name in table_names:
                    try:
                        # Get row count
                        count_result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                        row_count = count_result[0] if count_result else 0
                        
                        # Get current gameweek if applicable
                        current_gw = None
                        try:
                            gw_result = conn.execute(f"SELECT MAX(current_through_gameweek) FROM {table_name}").fetchone()
                            current_gw = gw_result[0] if gw_result and gw_result[0] else None
                        except:
                            pass  # Table doesn't have current_through_gameweek column
                        
                        status['table_details'][table_name] = {
                            'row_count': row_count,
                            'current_through_gameweek': current_gw
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
                        results[f"{category}_tables"] = "✅ All 3 tables exist"
                    else:
                        missing_count = tables_exist.count(False)
                        results[f"{category}_tables"] = f"❌ {missing_count} tables missing"
                
                # Check fixtures
                try:
                    fixture_result = conn.execute("SELECT COUNT(*) FROM raw_fixtures").fetchone()
                    if fixture_result is not None:
                        fixture_count = fixture_result[0]
                        results['fixtures'] = f"✅ {fixture_count} fixtures"
                    else:
                        results['fixtures'] = "❌ No fixture data returned"
                except:
                    results['fixtures'] = "❌ No fixtures table"
                
                # Sample data quality check
                try:
                    # Check a player table for data quality
                    player_result = conn.execute("SELECT COUNT(*) FROM player_standard").fetchone()
                    if player_result is not None:
                        player_count = player_result[0]
                        if player_count > 250:  # Reasonable number of Premier League players
                            results['player_data'] = f"✅ {player_count} players"
                        else:
                            results['player_data'] = f"⚠️ Only {player_count} players"
                    else:
                        results['player_data'] = "❌ No player data returned"
                except:
                    results['player_data'] = "❌ No player data"
                
            except Exception as e:
                results['validation_error'] = f"❌ Validation failed: {e}"
        
        return results
    
    def get_expected_stat_tables(self) -> List[str]:
        """Get list of expected stat table names"""
        categories = ['standard', 'keepers', 'keepersadv', 'shooting', 'passing', 
                     'passingtypes', 'goalshotcreation', 'defense', 'possession', 
                     'playingtime', 'misc']
        prefixes = ['squad', 'opponent', 'player']
        
        tables = []
        for prefix in prefixes:
            for category in categories:
                tables.append(f"{prefix}_{category}")
        
        return tables
    
    def check_stat_category_completion(self, category: str) -> Dict[str, Any]:
        """Check if all 3 tables exist for a stat category"""
        table_names = [f"squad_{category}", f"opponent_{category}", f"player_{category}"]
        
        with self.db.get_connection() as conn:
            results = {}
            
            for table_name in table_names:
                try:
                    count_result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                    count = count_result[0] if count_result else 0
                    results[table_name] = {
                        'exists': True,
                        'row_count': count
                    }
                except:
                    results[table_name] = {
                        'exists': False,
                        'row_count': 0
                    }
            
            # Summary
            all_exist = all(results[table]['exists'] for table in table_names)
            total_rows = sum(results[table]['row_count'] for table in table_names)
            
            results['summary'] = {
                'category': category,
                'all_tables_exist': all_exist,
                'total_rows': total_rows
            }
            
            return results