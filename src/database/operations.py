"""
Database operations for creating tables and inserting data
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
        """Create all necessary tables with proper schemas"""
        logger.info("Creating all database tables...")
        
        with self.db.get_connection() as conn:
            # Create infrastructure tables first
            self._create_gameweeks_table(conn)
            self._create_fixtures_table(conn)
            self._create_data_scraping_log_table(conn)
            self._create_teams_table(conn)
            
            # Create all stat tables (squad, opponent, player)
            table_names = self.db.get_table_names()
            
            for table_name in table_names["squad"]:
                self._create_stats_table(conn, table_name, "squad")
            
            for table_name in table_names["opponent"]:
                self._create_stats_table(conn, table_name, "opponent")
                
            for table_name in table_names["player"]:
                self._create_stats_table(conn, table_name, "player")
        
        logger.info("All tables created successfully")
    
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
        """Create fixtures table"""
        sql = """
        CREATE TABLE IF NOT EXISTS fixtures (
            fixture_id VARCHAR PRIMARY KEY,
            gameweek INTEGER,
            date DATE,
            time TIME,
            home_team VARCHAR,
            away_team VARCHAR,
            home_score INTEGER,
            away_score INTEGER,
            venue VARCHAR,
            referee VARCHAR,
            attendance INTEGER,
            is_completed BOOLEAN DEFAULT FALSE,
            scraped_date DATE DEFAULT CURRENT_DATE,
            FOREIGN KEY (gameweek) REFERENCES gameweeks(gameweek)
        )
        """
        conn.execute(sql)
        logger.debug("Created fixtures table")
    
    def _create_data_scraping_log_table(self, conn):
        """Create scraping log table"""
        sql = """
        CREATE TABLE IF NOT EXISTS data_scraping_log (
            id INTEGER PRIMARY KEY,
            table_name VARCHAR,
            gameweek INTEGER,
            scrape_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status VARCHAR, -- 'success', 'failed', 'partial'
            rows_scraped INTEGER,
            error_message TEXT,
            url VARCHAR
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
        Create a stats table with flexible schema
        table_type: 'squad', 'opponent', or 'player'
        """
        # Base columns that all tables have
        base_columns = [
            "id INTEGER PRIMARY KEY",
            "gameweek INTEGER",
            "scraped_date DATE DEFAULT CURRENT_DATE",
        ]
        
        # Add type-specific columns
        if table_type == "player":
            specific_columns = [
                "player_name VARCHAR",
                "nation VARCHAR", 
                "position VARCHAR",
                "squad VARCHAR",
                "age INTEGER",
            ]
        else:  # squad or opponent
            specific_columns = [
                "squad VARCHAR",
            ]
        
        # Common stat columns - we'll add these dynamically as data comes in
        # This flexible approach lets us handle any columns that FBRef throws at us
        dynamic_columns = [
            "-- Dynamic stat columns will be added when inserting data"
        ]
        
        all_columns = base_columns + specific_columns
        columns_sql = ",\n            ".join(all_columns)
        
        sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns_sql},
            FOREIGN KEY (gameweek) REFERENCES gameweeks(gameweek)
        )
        """
        
        conn.execute(sql)
        logger.debug(f"Created {table_name} table")
    
    def insert_data(self, table_name: str, data: pd.DataFrame, gameweek: int):
        """
        Insert data into a table with automatic schema adaptation
        This handles the fact that FBRef columns can vary
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
                # Check if table exists and get its columns
                existing_columns = self._get_table_columns(conn, table_name)
                
                # Add any new columns that don't exist
                new_columns = set(data.columns) - set(existing_columns)
                for col in new_columns:
                    self._add_column_if_not_exists(conn, table_name, col, data[col])
                
                # Insert the data - DuckDB connections work with pandas to_sql
                conn.register('temp_df', data)
                conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
                conn.unregister('temp_df')
                
                # Log success
                self._log_scraping_result(conn, table_name, gameweek, 'success', len(data), None, None)
                logger.info(f"Inserted {len(data)} rows into {table_name} for gameweek {gameweek}")
                
            except Exception as e:
                # Log failure
                self._log_scraping_result(conn, table_name, gameweek, 'failed', 0, str(e), None)
                logger.error(f"Failed to insert data into {table_name}: {e}")
                raise
    
    def _get_table_columns(self, conn, table_name: str) -> List[str]:
        """Get existing column names for a table"""
        try:
            result = conn.execute(f"DESCRIBE {table_name}").fetchall()
            return [row[0] for row in result]  # Column names are in first element
        except Exception:
            # Table might not exist yet
            return []
    
    def _add_column_if_not_exists(self, conn, table_name: str, column_name: str, column_data: pd.Series):
        """Add a column to table if it doesn't exist"""
        # Infer data type from pandas series
        if pd.api.types.is_integer_dtype(column_data):
            col_type = "INTEGER"
        elif pd.api.types.is_float_dtype(column_data):
            col_type = "FLOAT"
        elif pd.api.types.is_bool_dtype(column_data):
            col_type = "BOOLEAN"
        else:
            col_type = "VARCHAR"
        
        try:
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {col_type}")
            logger.debug(f"Added column {column_name} ({col_type}) to {table_name}")
        except Exception as e:
            # Column might already exist, which is fine
            if "already exists" not in str(e).lower():
                logger.warning(f"Could not add column {column_name} to {table_name}: {e}")
    
    def _log_scraping_result(self, conn, table_name: str, gameweek: int, status: str, 
                           rows_scraped: int, error_message: Optional[str] = None, url: Optional[str] = None):
        """Log scraping results"""
        log_data = {
            'table_name': table_name,
            'gameweek': gameweek,
            'status': status,
            'rows_scraped': rows_scraped,
            'error_message': error_message,
            'url': url
        }
        
        log_df = pd.DataFrame([log_data])
        conn.register('log_temp', log_df)
        conn.execute("INSERT INTO data_scraping_log SELECT * FROM log_temp")
        conn.unregister('log_temp')
    
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
        """Create a new gameweek entry"""
        with self.db.get_connection() as conn:
            # Use explicit column names to avoid the column count issue
            conn.execute("""
                INSERT INTO gameweeks (gameweek, start_date, end_date, is_complete) 
                VALUES (?, ?, ?, FALSE)
            """, (gameweek, start_date, end_date))
        
        logger.info(f"Created gameweek {gameweek}")
    
    def mark_gameweek_complete(self, gameweek: int):
        """Mark a gameweek as complete"""
        with self.db.get_connection() as conn:
            conn.execute("UPDATE gameweeks SET is_complete = TRUE WHERE gameweek = ?", (gameweek,))
        
        logger.info(f"Marked gameweek {gameweek} as complete")