import sys
import unittest
import sqlite3
import pandas as pd
from src.database import createConnection, insertData

class TestDatabase(unittest.TestCase):

    def setUp(self):
        self.db_file = ':memory:'  # Use in-memory database for testing
        self.conn = createConnection(self.db_file)
        self.test_df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})

    def tearDown(self):
        if self.conn:
            self.conn.close()

    def test_create_connection(self):
        self.assertIsNotNone(self.conn)
        self.assertIsInstance(self.conn, sqlite3.Connection)

    def test_insert_data(self):
        table_name = 'test_table'
        insertData(self.conn, self.test_df, table_name)
        
        # Verify data insertion
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0], (1, 'a'))

    def test_insert_data_empty_dataframe(self):
        empty_df = pd.DataFrame()
        table_name = 'empty_table'
        insertData(self.conn, empty_df, table_name)
        
        # Verify no table is created for empty DataFrame
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        result = cursor.fetchone()
        self.assertIsNone(result)

if __name__ == '__main__':
    unittest.main()