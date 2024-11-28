import unittest
import pandas as pd
from datetime import datetime
from src.datacleaner import cleanData

class TestCleanDataFunction(unittest.TestCase):

    def test_clean_data_empty_list(self):
        # Test with an empty list
        rawdata = []
        with self.assertRaises(ValueError):
            cleanData(rawdata)

    def test_clean_data_invalid_input(self):
        # Test with invalid input (not a list of DataFrames)
        rawdata = ['string', 123, None]
        with self.assertRaises(AttributeError):
            cleanData(rawdata)

    def test_clean_data_single_dataframe(self):
        # Test with a single DataFrame (this should raise an error)
        rawdata = [pd.DataFrame({
            'level_0 Column': [1, 2, 3],
            'Another Column': [4, 5, 6]
        })]
        with self.assertRaises(ValueError):
            cleanData(rawdata)

    def test_clean_data_multiple_dataframes(self):
        # Test with multiple DataFrames
        rawdata = [
            pd.DataFrame({
                'level_0 Column': [1, 2, 3],
                'Another Column': [4, 5, 6]
            }),
            pd.DataFrame({
                'level_0 Column': [7, 8, 9],
                'Another Column': [10, 11, 12]
            }),
            pd.DataFrame({
                'Age': ['20', '30', '40'],
                'Nation': ['USA New York', 'Canada Ontario', 'Mexico City'],
                'Rk': [1, 2, 3],
                'Matches': [10, 20, 30],
                'Goals': [5, 10, 15]
            })
        ]
        cleaned_data = cleanData(rawdata)
        self.assertEqual(len(cleaned_data), 3)
        self.assertIn('Current Date', cleaned_data[0].columns)
        self.assertIn('Current Date', cleaned_data[1].columns)
        self.assertIn('Current Date', cleaned_data[2].columns)

        # Check specific transformations for PlayerStats
        if 'Age' in cleaned_data[2].columns:
            self.assertEqual(cleaned_data[2]['Age'].dtype, object)  # Age is string after slicing
        if 'Nation' in cleaned_data[2].columns:
            self.assertEqual(cleaned_data[2]['Nation'].dtype, object)  # Nation is string after splitting
        self.assertNotIn('Rk', cleaned_data[2].columns)  # Rk and Matches should be dropped
        self.assertNotIn('Matches', cleaned_data[2].columns)

    def test_clean_data_numeric_conversion(self):
        # Test numeric conversion
        rawdata = [
            pd.DataFrame({
                'level_0 Column': ['1', '2', '3'],
                'Another Column': ['4', '5', '6']
            }),
            pd.DataFrame({
                'level_0 Column': ['7', '8', '9'],
                'Another Column': ['10', '11', '12']
            }),
            pd.DataFrame({
                'Age': ['20 years', '30 years', '40 years'],
                'Nation': ['USA New York', 'Canada Ontario', 'Mexico City'],
                'Rk': ['1', '2', '3'],
                'Matches': ['10', '20', '30'],
                'Goals': ['5', '10', '15']
            })
        ]
        cleaned_data = cleanData(rawdata)
        for col in cleaned_data[0].columns[1:-1]:
            self.assertIn(cleaned_data[0][col].dtype, [int, float])  # Converted to numeric
        for col in cleaned_data[1].columns[1:-1]:
            self.assertIn(cleaned_data[1][col].dtype, [int, float])  # Converted to numeric
        for col in cleaned_data[2].columns[4:-1]:
            self.assertIn(cleaned_data[2][col].dtype, [int, float])  # Converted to numeric

if __name__ == '__main__':
    unittest.main()