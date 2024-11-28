import unittest
from unittest.mock import patch, MagicMock
from src.webscraper import scrapeData

class TestWebScraper(unittest.TestCase):

    @patch('src.webscraper.requests.get')
    def test_scrape_data_success(self, mock_get):
        # Mock a successful HTTP response with a table in the main content and one in comments
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = """
        <html>
            <body>
                <table><tr><td>Test Data</td></tr></table>
                <!-- <div class="table_container"><table><tr><td>Comment Table</td></tr></table></div> -->
            </body>
        </html>
        """
        mock_get.return_value = mock_response

        # Call the function with a fake URL
        result = scrapeData('http://fakeurl.com')

        # Assertions to check if tables are extracted correctly
        self.assertEqual(len(result), 2)  # Expecting two tables
        self.assertEqual(result[0].iloc[0, 0], 'Test Data')
        self.assertEqual(result[1].iloc[0, 0], 'Comment Table')

    @patch('src.webscraper.requests.get')
    def test_scrape_data_failure(self, mock_get):
        # Mock a failed HTTP response
        mock_get.side_effect = Exception("Request failed")

        # Call the function with a fake URL and expect an empty list due to failure
        result = scrapeData('http://fakeurl.com')
        
        # Assertions to check if no tables are extracted on failure
        self.assertEqual(len(result), 0)

if __name__ == '__main__':
    unittest.main()