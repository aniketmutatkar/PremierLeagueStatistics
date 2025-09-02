"""
Raw Database Package - Raw data storage following archive pattern
"""
from .connection import RawDatabaseConnection
from .operations import RawDatabaseOperations

def initialize_raw_database(config_path: str = "config/database.yaml"):
    """
    Initialize the raw database connection and operations
    
    Returns:
        tuple: (RawDatabaseConnection, RawDatabaseOperations)
    """
    # Create connection
    db_connection = RawDatabaseConnection(config_path)
    
    # Create operations handler
    db_operations = RawDatabaseOperations(db_connection)
    
    # Create infrastructure tables
    db_operations.create_infrastructure_tables()
    
    return db_connection, db_operations

__all__ = ['RawDatabaseConnection', 'RawDatabaseOperations', 'initialize_raw_database']