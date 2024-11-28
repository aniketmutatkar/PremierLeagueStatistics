import sqlite3
from sqlite3 import Error
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def createConnection(dbFile):
    """Create a database connection to the SQLite database specified by db_file."""
    try:
        conn = sqlite3.connect(dbFile)
        logger.info(f"Connected to SQLite database: {dbFile}")
        return conn
    except Error as e:
        logger.error(f"Error connecting to database: {e}")
    return None

def insertData(conn, df, table_name):
    """Create a table and insert data from DataFrame into SQLite database."""
    try:
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        logger.info(f"Table '{table_name}' created and populated successfully.")
    except Error as e:
        logger.error(f"Error inserting data into table '{table_name}': {e}")