import requests
from bs4 import BeautifulSoup, Comment
import pandas as pd
import logging
from io import StringIO  # Import StringIO for wrapping HTML

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def scrapeData(url):
    try:
        # Make a request to the website and parse HTML content
        # response = requests.get(url)
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