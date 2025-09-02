"""
Database operations for creating tables and inserting data - FIXED VERSION
"""
import pandas as pd
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from .connection import DatabaseConnection

logger = logging.getLogger(__name__)

class DatabaseOperations:
    """Handles database table creation and data operations"""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection
    
    def create_all_tables(self):
        """Create only infrastructure tables - stat tables will be created dynamically"""
        logger.info("Creating infrastructure tables...")
        
        with self.db.get_connection() as conn:
            # Create infrastructure tables only
            self._create_gameweeks_table(conn)
            self._create_fixtures_table(conn)
            self._create_teams_table(conn)
            # Skip data_scraping_log for now - we'll just log to console
            
        logger.info("Infrastructure tables created successfully")
    
    def _create_gameweeks_table(self, conn):
        """Create gameweeks tracking table"""
        sql = """
        CREATE TABLE IF NOT EXISTS gameweeks (
            gameweek INTEGER PRIMARY KEY,
            start_date DATE,
            end_date DATE,
            is_complete BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        conn.execute(sql)
        logger.debug("Created gameweeks table")
    
    def _create_fixtures_table(self, conn):
        """Enhanced fixtures table that includes gameweek metadata"""
        sql = """
        CREATE TABLE IF NOT EXISTS current_fixtures (
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
            -- Gameweek metadata
            gameweek_start_date DATE,
            gameweek_end_date DATE,
            gameweek_is_complete BOOLEAN DEFAULT FALSE,
            scraped_date DATE DEFAULT CURRENT_DATE
        )
        """
        conn.execute(sql)
        logger.debug("Created current_fixtures table")
    
    def create_current_state_tables(self):
        """Create tables for current state only (5 tables total)"""
        logger.info("Creating current state tables...")
        
        with self.db.get_connection() as conn:
            # Infrastructure tables (unchanged)
            self._create_fixtures_table(conn)  # Combines fixtures + gameweeks info
            self._create_teams_table(conn)
            
            # Note: Stats tables will be created dynamically when data is inserted
            # This allows for flexible column structures from consolidated data
        
        logger.info("Current state tables created successfully")

    def _create_data_scraping_log_table(self, conn):
        """Create scraping log table - FIXED to match the 8 columns we're inserting"""
        sql = """
        CREATE TABLE IF NOT EXISTS data_scraping_log (
            table_name VARCHAR,
            gameweek INTEGER,
            scrape_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status VARCHAR, -- 'success', 'failed', 'partial'
            rows_scraped INTEGER,
            error_message TEXT,
            url VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        conn.execute(sql)
        logger.debug("Created data_scraping_log table")
    
    def _create_teams_table(self, conn):
        """Create teams table"""
        sql = """
        CREATE TABLE IF NOT EXISTS teams (
            team_id INTEGER PRIMARY KEY,
            team_name VARCHAR UNIQUE,
            short_name VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        conn.execute(sql)
        logger.debug("Created teams table")
    
    def _create_stats_table(self, conn, table_name: str, table_type: str):
        """
        Create a stats table - SIMPLIFIED VERSION
        Let pandas to_sql handle the schema creation
        """
        # Just create a minimal table - pandas will add columns as needed
        base_columns = [
            "id INTEGER PRIMARY KEY",
            "gameweek INTEGER",
            "scraped_date DATE DEFAULT CURRENT_DATE"
        ]
        
        if table_type == "player":
            specific_columns = [
                "Player VARCHAR",
                "Nation VARCHAR", 
                "Pos VARCHAR",
                "Squad VARCHAR",
                "Age VARCHAR"
            ]
        else:  # squad or opponent
            specific_columns = [
                "Squad VARCHAR"
            ]
        
        all_columns = base_columns + specific_columns
        columns_sql = ",\n            ".join(all_columns)
        
        sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns_sql}
        )
        """
        
        conn.execute(sql)
        logger.debug(f"Created {table_name} table")
    
    def insert_current_state_data(self, table_name: str, data: pd.DataFrame, current_gameweek: int):
        """
        Insert data with current state approach (overwrite existing)
        """
        if data.empty:
            logger.warning(f"No data to insert for {table_name}")
            return
        
        # Add metadata
        data = data.copy()
        data['current_through_gameweek'] = current_gameweek
        data['last_updated'] = datetime.now().date()
        
        with self.db.get_connection() as conn:
            try:
                # Drop table if it exists (overwrite approach)
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                
                # Create new table with current data structure
                conn.register('create_table_data', data)
                conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM create_table_data")
                conn.unregister('create_table_data')
                
                logger.info(f"✅ Overwrote {table_name} with {len(data)} rows for gameweek {current_gameweek}")
                
            except Exception as e:
                logger.error(f"❌ Failed to insert current state data into {table_name}: {e}")
                raise

    def insert_data(self, table_name: str, data: pd.DataFrame, gameweek: int):
        """
        Insert data using DuckDB - create table if it doesn't exist
        """
        if data.empty:
            logger.warning(f"No data to insert for {table_name}")
            return
        
        # Add our tracking columns
        data = data.copy()
        data['gameweek'] = gameweek
        data['scraped_date'] = datetime.now().date()
        
        with self.db.get_connection() as conn:
            try:
                # Check if table exists, if not create it with the data structure
                table_exists = self._table_exists(conn, table_name)
                
                if not table_exists:
                    # Create table using the first row of data to determine structure
                    conn.register('create_table_data', data.head(1))
                    conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM create_table_data WHERE 1=0")  # Empty table with structure
                    conn.unregister('create_table_data')
                    logger.info(f"Created table {table_name} with {len(data.columns)} columns")
                
                # Insert the data
                conn.register('temp_data', data)
                conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_data")
                conn.unregister('temp_data')
                
                # Log success (simplified - just log to console for now)
                logger.info(f"✅ Inserted {len(data)} rows into {table_name} for gameweek {gameweek}")
                
            except Exception as e:
                logger.error(f"❌ Failed to insert data into {table_name}: {e}")
                raise
    
    def _table_exists(self, conn, table_name: str) -> bool:
        """Check if a table exists"""
        try:
            result = conn.execute(f"SELECT COUNT(*) FROM {table_name} LIMIT 1").fetchone()
            return True
        except Exception:
            return False
    
    def _log_scraping_result(self, conn, table_name: str, gameweek: int, status: str, 
                           rows_scraped: int, error_message: Optional[str] = None, url: Optional[str] = None):
        """Log scraping results using DuckDB native approach"""
        try:
            # Create log entry
            log_data = pd.DataFrame([{
                'table_name': table_name,
                'gameweek': gameweek,
                'status': status,
                'rows_scraped': rows_scraped,
                'error_message': error_message,
                'url': url,
                'scrape_date': datetime.now()
            }])
            
            conn.register('temp_log', log_data)
            conn.execute("INSERT INTO data_scraping_log SELECT * FROM temp_log")
            conn.unregister('temp_log')
            
        except Exception as e:
            logger.warning(f"Failed to log scraping result: {e}")
    
    def get_current_gameweek(self) -> int:
        """Get the current/latest gameweek"""
        with self.db.get_connection() as conn:
            try:
                result = conn.execute("SELECT MAX(gameweek) FROM gameweeks WHERE is_complete = FALSE").fetchone()
                if result is not None and result[0] is not None:
                    return result[0]
                else:
                    return 1
            except Exception:
                # If no gameweeks exist, start with 1
                return 1
    
    def create_gameweek(self, gameweek: int, start_date: Optional[str] = None, end_date: Optional[str] = None):
        """Create a new gameweek entry using DuckDB native approach"""
        with self.db.get_connection() as conn:
            # Use explicit column names to avoid the column count issue
            conn.execute("""
                INSERT INTO gameweeks (gameweek, start_date, end_date, is_complete) 
                VALUES (?, ?, ?, ?)
            """, (gameweek, start_date, end_date, False))
        
        logger.info(f"Created gameweek {gameweek}")
    
    def mark_gameweek_complete(self, gameweek: int):
        """Mark a gameweek as complete"""
        with self.db.get_connection() as conn:
            conn.execute("UPDATE gameweeks SET is_complete = TRUE WHERE gameweek = ?", (gameweek,))
        
        logger.info(f"Marked gameweek {gameweek} as complete")
    
    def get_current_database_status(self) -> Dict[str, Any]:
        """Get status of current database"""
        with self.db.get_connection() as conn:
            # Get all table names
            tables_result = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()
            table_names = [row[0] for row in tables_result]
            
            status = {
                'total_tables': len(table_names),
                'table_names': table_names,
                'table_details': {}
            }
            
            # Get details for each table
            for table_name in table_names:
                try:
                    count_result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                    row_count = count_result[0] if count_result else 0
                    
                    # Get current gameweek if applicable
                    current_gw = None
                    if 'current_through_gameweek' in conn.execute(f"PRAGMA table_info({table_name})").fetchall():
                        gw_result = conn.execute(f"SELECT MAX(current_through_gameweek) FROM {table_name}").fetchone()
                        current_gw = gw_result[0] if gw_result and gw_result[0] else None
                    
                    status['table_details'][table_name] = {
                        'row_count': row_count,
                        'current_through_gameweek': current_gw
                    }
                    
                except Exception as e:
                    status['table_details'][table_name] = {'error': str(e)}
            
            return status