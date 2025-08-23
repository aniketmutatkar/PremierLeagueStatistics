"""
Database package initialization
"""
from .connection import DatabaseConnection
from .operations import DatabaseOperations

def initialize_database(config_path: str = "config/database.yaml"):
    """
    Initialize the database connection and create all tables
    
    Returns:
        tuple: (DatabaseConnection, DatabaseOperations)
    """
    # Create connection
    db_connection = DatabaseConnection(config_path)
    
    # Create operations handler
    db_operations = DatabaseOperations(db_connection)
    
    # Create all tables
    db_operations.create_all_tables()
    
    return db_connection, db_operations

__all__ = ['DatabaseConnection', 'DatabaseOperations', 'initialize_database']