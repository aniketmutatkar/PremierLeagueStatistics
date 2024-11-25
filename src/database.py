import sqlite3
from sqlite3 import Error
import pandas as pd
import logging
from datacleaner import cleanData
from webscraper import scrapeData

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

if __name__ == "__main__":
    # TESTING: Use txt file with website html
    url = 'data/test_html.txt'
    # url = 'https://fbref.com/en/comps/9/stats/Premier-League-Stats'  # Standard Stats
    logging.info(f"Scraping data from {url}.")
    
    rawdata = scrapeData(url)
    
    if rawdata:
        logging.info(f"Extracted {len(rawdata)} raw tables.")
    else:
        logging.warning("No data extracted.")

    # Get cleaned data from datacleaner.py
    SquadStandardStats, OpponentStandardStats, PlayerStandardStats = cleanData(rawdata)
    
    if not SquadStandardStats.empty and not OpponentStandardStats.empty and not PlayerStandardStats.empty:
        logging.info("Extracted clean tables.")
    else:
        logging.warning("No clean data extracted or some tables are empty.")
    
    dbFile = 'data/testdata.db'
    
    # Create connection
    connection = createConnection(dbFile)
    
    if connection:
        # Create tables and insert data
        insertData(connection, SquadStandardStats, 'SquadStandardStats')
        insertData(connection, OpponentStandardStats, 'OpponentStandardStats')
        insertData(connection, PlayerStandardStats, 'PlayerStandardStats')
        
        logger.info("All data inserted successfully.")
        
        connection.close()
    else:
        logger.error("Failed to create database connection.")