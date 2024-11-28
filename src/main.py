import logging
from ratelimiter import RateLimiter
from datacleaner import cleanData
from webscraper import scrapeData
from database import createConnection, insertData

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define a rate limiter: 1 request per 60 second
rate_limiter = RateLimiter(max_calls=1, period=10)

def main():
    # Define an array of URLs for scraping
    urls = [
        #'data/Standard_html.txt',  # For testing with local HTML file
        #'data/Shooting_html.txt',  # For testing with local HTML file
        #'data/Passing_html.txt'    # For testing with local HTML file

        # Uncomment the following lines for live scraping
        'https://fbref.com/en/comps/9/stats/Premier-League-Stats',
        'https://fbref.com/en/comps/9/keepers/Premier-League-Stats',
        'https://fbref.com/en/comps/9/keepersadv/Premier-League-Stats',
        'https://fbref.com/en/comps/9/shooting/Premier-League-Stats',
        'https://fbref.com/en/comps/9/passing/Premier-League-Stats',
        'https://fbref.com/en/comps/9/passing_types/Premier-League-Stats',
        'https://fbref.com/en/comps/9/gca/Premier-League-Stats',
        'https://fbref.com/en/comps/9/defense/Premier-League-Stats',
        'https://fbref.com/en/comps/9/possession/Premier-League-Stats',
        'https://fbref.com/en/comps/9/playingtime/Premier-League-Stats',
        'https://fbref.com/en/comps/9/misc/Premier-League-Stats'
    ]

    tableNames = [
        ['squad_standard', 'opponent_standard', 'player_standard'],
        ['squad_keepers', 'opponent_keepers', 'player_keepers'],
        ['squad_keepersadv', 'opponent_keepersadv', 'player_keepersadv'],
        ['squad_shooting', 'opponent_shooting', 'player_shooting'],
        ['squad_passing', 'opponent_passing', 'player_passing'],
        ['squad_passingtypes', 'opponent_passingtypes', 'player_passingtypes'],
        ['squad_goalshotcreation', 'opponent_goalshotcreation', 'player_goalshotcreation'],
        ['squad_defense', 'opponent_defense', 'player_defense'],
        ['squad_possession', 'opponent_possession', 'player_possession'],
        ['squad_playingtime', 'opponent_playingtime', 'player_playingtime'],
        ['squad_misc', 'opponent_misc', 'player_misc']
    ]

    for i, url in enumerate(urls):
        logger.info(f"Scraping data from {url}.")
        
        with rate_limiter:  # Rate limit applied here
            # Step 1: Scrape Data
            raw_data = scrapeData(url)
        
        if raw_data:
            logger.info(f"Extracted {len(raw_data)} raw tables from {url}.")
        else:
            logger.warning(f"No data extracted from {url}.")
            continue  # Skip to the next URL

        # Step 2: Clean Data
        SquadStats, OpponentStats, PlayerStats = cleanData(raw_data)
        
        if not SquadStats.empty and not OpponentStats.empty and not PlayerStats.empty:
            logger.info(f"Extracted clean tables from {url}.")
        else:
            logger.warning(f"No clean data extracted or some tables are empty from {url}.")
            continue  # Skip to the next URL
        
        # Step 3: Store Data in SQLite Database
        db_file = 'data/rawdata.db'
        
        # Create database connection
        connection = createConnection(db_file)
        
        if connection:
            # Insert cleaned data into database tables using tableNames
            insertData(connection, SquadStats, tableNames[i][0])  # Insert Squad Stats
            insertData(connection, OpponentStats, tableNames[i][1])  # Insert Opponent Stats
            insertData(connection, PlayerStats, tableNames[i][2])  # Insert Player Stats
            
            logger.info("All data inserted successfully.")
            
            # Close the database connection
            connection.close()
        else:
            logger.error("Failed to create database connection.")

if __name__ == "__main__":
    main()