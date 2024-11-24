import sqlite3

def create_connection(db_file):
    """Create a database connection to the SQLite database specified by db_file."""
    conn = sqlite3.connect(db_file)
    return conn

def create_table(conn):
    """Create a table in the database if it does not exist."""
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            link TEXT NOT NULL
        )
    ''')
    conn.commit()

def insert_data(conn, data):
    """Insert cleaned data into the articles table."""
    cursor = conn.cursor()
    
    # Prepare data for insertion
    articles_to_insert = [(row['title'], row['link']) for row in data]
    
    cursor.executemany('INSERT INTO articles (title, link) VALUES (?, ?)', articles_to_insert)
    conn.commit()

if __name__ == "__main__":
    db_file = 'data/articles.db'
    
    # Sample usage of database functions
    connection = create_connection(db_file)
    
    if connection:
        create_table(connection)
        
        sample_data = [{'title': 'Article 1', 'link': 'http://example.com/article1'},
                       {'title': 'Article 2', 'link': 'http://example.com/article2'}]
        
        insert_data(connection, sample_data)
        print("Data inserted successfully.")
        
        connection.close()