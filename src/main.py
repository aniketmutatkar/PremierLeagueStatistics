import logging
from datacleaner import cleanData
from webscraper import scrapeData
from database import createConnection, insertData

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    # Define the URL or file path for testing
    url = 'data/test_html.txt'  # For testing with local HTML file
    # url = 'https://fbref.com/en/comps/9/stats/Premier-League-Stats'  # Uncomment for live scraping

    logger.info(f"Scraping data from {url}.")
    
    # Step 1: Scrape Data
    raw_data = scrapeData(url)
    
    if raw_data:
        logger.info(f"Extracted {len(raw_data)} raw tables.")
    else:
        logger.warning("No data extracted.")
        return

    # Step 2: Clean Data
    SquadStandardStats, OpponentStandardStats, PlayerStandardStats = cleanData(raw_data)
    
    if not SquadStandardStats.empty and not OpponentStandardStats.empty and not PlayerStandardStats.empty:
        logger.info("Extracted clean tables.")
    else:
        logger.warning("No clean data extracted or some tables are empty.")
        return
    
    # Step 3: Store Data in SQLite Database
    db_file = 'data/testdata.db'
    
    # Create database connection
    connection = createConnection(db_file)
    
    if connection:
        # Insert cleaned data into database tables
        insertData(connection, SquadStandardStats, 'SquadStandardStats')
        insertData(connection, OpponentStandardStats, 'OpponentStandardStats')
        insertData(connection, PlayerStandardStats, 'PlayerStandardStats')
        
        logger.info("All data inserted successfully.")
        
        # Close the database connection
        connection.close()
    else:
        logger.error("Failed to create database connection.")

if __name__ == "__main__":
    main()