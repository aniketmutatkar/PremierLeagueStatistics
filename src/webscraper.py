import requests
from bs4 import BeautifulSoup, Comment
import pandas as pd
import logging
import random
from io import StringIO  # Import StringIO for wrapping HTML

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def scrapeData(url):
    # List of user agents to rotate through and randomly select a user agent from the list
    user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15',
    'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Mobile Safari/537.3',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 16_6_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.',
    ]
    user_agent = random.choice(user_agents)

    try:
        headers = {
        'User-Agent': user_agent,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive'
        }
        
        # Make a request to the website and parse HTML content
        # response = requests.get(url, headers=headers)
        # response.raise_for_status()  # Raise an error for bad responses
        
        # TESTING: Open and read the contents of the HTML file
        with open(url, 'r') as file:
            html = file.read()
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Initialize an empty list to store DataFrames
        dfs = []

        # Find all table tags
        tables = soup.find_all('table')

        # Loop through each table, convert it to a DataFrame, and append it to the list
        for table in tables:
            df = pd.read_html(StringIO(str(table)))[0]  # Wrap in StringIO
            dfs.append(df)
            logging.info("Extracted table from main content.")

        # Find all tables within the page's comments
        comments = soup.find_all(string=lambda text: isinstance(text, Comment))

        for comment in comments:
            comment_content = str(comment)  # Store comment content as string
            
            # Check if comment content has any HTML tags before parsing
            if '<' in comment_content and '>' in comment_content:
                comment_soup = BeautifulSoup(comment_content, 'html.parser')
                # Now you can use 'comment_soup' to find elements within this comment
                table_container = comment_soup.find('div', class_='table_container')
                
                if table_container:
                    df = pd.read_html(StringIO(str(table_container)))[0]  # Wrap in StringIO
                    dfs.append(df)
                    logging.info("Extracted table from comment content.")

    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
    except ValueError as e:
        logging.error(f"Value error while processing data: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

    return dfs if 'dfs' in locals() else []

if __name__ == "__main__":
    # TESTING: Use txt file with website html
    url = 'data/test_html.txt'

    # url = 'https://fbref.com/en/comps/9/stats/Premier-League-Stats'  # Standard Stats
    data = scrapeData(url)
    
    if data:
        print(f"Extracted {len(data)} tables.")
    else:
        print("No data extracted.")