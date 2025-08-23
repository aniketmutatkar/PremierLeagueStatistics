"""
Database connection and configuration management for DuckDB
"""
import duckdb
import yaml
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class DatabaseConnection:
    """Manages DuckDB connection and configuration"""
    
    def __init__(self, config_path: str = "config/database.yaml"):
        self.config = self._load_config(config_path)
        self.db_path = self.config["database"]["path"]
        self.connection_settings = self.config["database"]["connection"]
        self._connection: Optional[duckdb.DuckDBPyConnection] = None
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load database configuration from YAML file"""
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
                logger.info(f"Loaded database config from {config_path}")
                return config
        except FileNotFoundError:
            logger.error(f"Config file not found: {config_path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing config file: {e}")
            raise
    
    def connect(self) -> duckdb.DuckDBPyConnection:
        """Create and configure DuckDB connection"""
        if self._connection is None:
            try:
                # Ensure data directory exists
                Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
                
                # Create connection
                self._connection = duckdb.connect(self.db_path)
                
                # Apply connection settings
                memory_limit = self.connection_settings.get("memory_limit", "1GB")
                threads = self.connection_settings.get("threads", 4)
                
                self._connection.execute(f"SET memory_limit='{memory_limit}'")
                self._connection.execute(f"SET threads={threads}")
                
                logger.info(f"Connected to DuckDB at {self.db_path}")
                logger.info(f"Applied settings: memory_limit={memory_limit}, threads={threads}")
                
            except Exception as e:
                logger.error(f"Failed to connect to DuckDB: {e}")
                raise
        
        return self._connection
    
    def disconnect(self):
        """Close DuckDB connection"""
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("Disconnected from DuckDB")
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = self.connect()
        try:
            yield conn
        finally:
            # Don't close the connection here - keep it alive for reuse
            pass
    
    def get_table_names(self) -> Dict[str, list]:
        """Get all table names from config"""
        tables = self.config["database"]["tables"]
        return {
            "squad": tables["squad_tables"],
            "opponent": tables["opponent_tables"], 
            "player": tables["player_tables"],
            "infrastructure": tables["infrastructure_tables"]
        }
    
    def get_all_stat_tables(self) -> list:
        """Get all stat table names (squad + opponent + player)"""
        tables = self.get_table_names()
        return tables["squad"] + tables["opponent"] + tables["player"]
    
    def __del__(self):
        """Cleanup connection on object destruction"""
        self.disconnect()