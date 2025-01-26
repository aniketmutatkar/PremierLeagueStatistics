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

def read_data(conn, query):
    """Execute a SQL query and return the result as a pandas DataFrame."""
    try:
        # Execute the query and fetch the results into a DataFrame
        df = pd.read_sql_query(query, conn)
        
        # Log the successful query execution
        logger.info(f"Query executed successfully. Returned {len(df)} rows.")
        
        return df
    except Error as e:
        # Log any errors that occur during query execution
        logger.error(f"Error executing query: {e}")
        return None

