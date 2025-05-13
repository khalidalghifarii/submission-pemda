import unittest
import pandas as pd
import os
import sys
from unittest.mock import patch, MagicMock

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the extract functions
from utils.extract import scrape_page, get_next_page_url, scrape_main

class TestExtract(unittest.TestCase):
    
    def test_scrape_page_returns_list(self):
        """Test that scrape_page returns a list"""
        # Mock the requests.Session and its get method
        with patch('requests.Session') as mock_session:
            # Create a mock response
            mock_response = MagicMock()
            mock_response.text = """
            <html>
                <body>
                    <div class="collection-card">
                        <div class="product-details">
                            <h3 class="product-title">Test Product</h3>
                            <span>$19.99</span>
                            <p>Rating: ⭐ 4.5 / 5</p>
                            <p>3 Colors</p>
                            <p>Size: M</p>
                            <p>Gender: Unisex</p>
                        </div>
                    </div>
                </body>
            </html>
            """
            mock_response.raise_for_status = MagicMock()
            
            # Set up the mock session to return our mock response
            mock_session_instance = MagicMock()
            mock_session_instance.get.return_value = mock_response
            mock_session.return_value = mock_session_instance
            
            # Call the function with a test URL
            result = scrape_page("http://test-url.com", mock_session_instance)
            
            # Assert the result is a list
            self.assertIsInstance(result, list)
            
            # The mock should have been called once with the URL
            mock_session_instance.get.assert_called_once_with("http://test-url.com")
    
    def test_get_next_page_url(self):
        """Test extraction of next page URL"""
        # Create a mock BeautifulSoup object
        from bs4 import BeautifulSoup
        
        # HTML with a next page link
        html_with_next = """
        <html>
            <body>
                <a class="page-link" href="/page/2">Next</a>
            </body>
        </html>
        """
        
        # HTML without a next page link
        html_without_next = """
        <html>
            <body>
                <a class="page-link" href="/page/2">Previous</a>
            </body>
        </html>
        """
        
        # Parse the HTML
        soup_with_next = BeautifulSoup(html_with_next, 'html.parser')
        soup_without_next = BeautifulSoup(html_without_next, 'html.parser')
        
        # Test with a next page link
        result_with_next = get_next_page_url("https://fashion-studio.dicoding.dev", soup_with_next)
        self.assertEqual(result_with_next, "https://fashion-studio.dicoding.dev/page/2")
        
        # Test without a next page link
        result_without_next = get_next_page_url("https://fashion-studio.dicoding.dev", soup_without_next)
        self.assertIsNone(result_without_next)
    
    @patch('utils.extract.scrape_page')
    @patch('utils.extract.requests.Session')
    def test_scrape_main(self, mock_session, mock_scrape_page):
        """Test the main scraping function"""
        # Mock session and response
        mock_session_instance = MagicMock()
        mock_session.return_value = mock_session_instance
        
        mock_response = MagicMock()
        mock_response.text = """
        <html>
            <body>
                <a class="page-link" href="/page/2">Next</a>
            </body>
        </html>
        """
        mock_response.raise_for_status = MagicMock()
        mock_session_instance.get.return_value = mock_response
        
        # Mock the scrape_page function to return a list of products
        mock_scrape_page.return_value = [
            {
                'Title': 'Test Product 1',
                'Price': '$19.99',
                'Rating': 'Rating: ⭐ 4.5 / 5',
                'Colors': '3 Colors',
                'Size': 'Size: M',
                'Gender': 'Gender: Unisex',
                'timestamp': '2025-05-13 14:41:10'
            },
            {
                'Title': 'Test Product 2',
                'Price': '$29.99',
                'Rating': 'Rating: ⭐ 3.5 / 5',
                'Colors': '2 Colors',
                'Size': 'Size: L',
                'Gender': 'Gender: Men',
                'timestamp': '2025-05-13 14:41:10'
            }
        ]
        
        # Call the scrape_main function with a maximum of 1 page
        result = scrape_main(max_pages=1)
        
        # Assert that scrape_page was called once
        mock_scrape_page.assert_called_once()
        
        # Assert that the result is a DataFrame with the expected columns
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 2)  # Two products
        self.assertIn('Title', result.columns)
        self.assertIn('Price', result.columns)
        self.assertIn('Rating', result.columns)
        self.assertIn('Colors', result.columns)
        self.assertIn('Size', result.columns)
        self.assertIn('Gender', result.columns)
        self.assertIn('timestamp', result.columns)

if __name__ == '__main__':
    unittest.main()