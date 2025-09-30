"""
Analytics Database Connection - Manages connections to analytics database
"""
import duckdb
import yaml
import logging
from pathlib import Path
from typing import Optional, Tuple
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class AnalyticsDBConnection:
    """Manages connections to analytics database and ETL operations"""
    
    def __init__(self, config_path: str = "config/database.yaml"):
        self.config = self._load_config(config_path)
        self.raw_db_path = self.config['database']['paths']['raw']
        self.analytics_db_path = self.config['database']['paths']['analytics']
        
    def _load_config(self, config_path: str) -> dict:
        """Load database configuration"""
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    
    @contextmanager
    def get_raw_connection(self):
        """Get read-only connection to raw database for ETL input"""
        conn = None
        try:
            conn = duckdb.connect(self.raw_db_path, read_only=True)
            logger.info(f"Connected to raw database: {self.raw_db_path}")
            yield conn
        except Exception as e:
            logger.error(f"Error connecting to raw database: {e}")
            raise
        finally:
            if conn:
                conn.close()
                logger.debug("Raw database connection closed")
    
    @contextmanager 
    def get_analytics_connection(self):
        """Get read/write connection to analytics database"""
        conn = None
        try:
            conn = duckdb.connect(self.analytics_db_path, read_only=False)
            logger.info(f"Connected to analytics database: {self.analytics_db_path}")
            yield conn
        except Exception as e:
            logger.error(f"Error connecting to analytics database: {e}")
            raise
        finally:
            if conn:
                conn.close()
                logger.debug("Analytics database connection closed")
    
    @contextmanager
    def get_dual_connections(self):
        """Get both raw (read-only) and analytics (read/write) connections for ETL"""
        raw_conn = None
        analytics_conn = None
        try:
            raw_conn = duckdb.connect(self.raw_db_path, read_only=True)
            analytics_conn = duckdb.connect(self.analytics_db_path, read_only=False)
            logger.info("Dual connections established for ETL")
            yield raw_conn, analytics_conn
        except Exception as e:
            logger.error(f"Error establishing dual connections: {e}")
            raise
        finally:
            if raw_conn:
                raw_conn.close()
            if analytics_conn:
                analytics_conn.close()
            logger.debug("Dual connections closed")
    
    def validate_connections(self) -> Tuple[bool, bool]:
        """Validate both database connections are working"""
        raw_ok = False
        analytics_ok = False
        
        try:
            with self.get_raw_connection() as conn:
                result = conn.execute("SELECT COUNT(*) FROM player_standard").fetchone()
                raw_ok = result[0] > 0
                logger.info(f"Raw DB validation: {result[0]} players found")
        except Exception as e:
            logger.error(f"Raw DB validation failed: {e}")
        
        try:
            with self.get_analytics_connection() as conn:
                result = conn.execute("SELECT COUNT(*) FROM analytics_players").fetchone()
                analytics_ok = True  # Connection worked even if table is empty
                logger.info(f"Analytics DB validation: {result[0]} records found")
        except Exception as e:
            logger.error(f"Analytics DB validation failed: {e}")
        
        return raw_ok, analytics_ok