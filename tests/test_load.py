import unittest
import pandas as pd
import os
import sys
from unittest.mock import patch, MagicMock, mock_open

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the load functions
from utils.load import save_to_csv, save_to_google_sheets, save_to_postgresql, load_data

class TestLoad(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures"""
        # Create a sample DataFrame for testing
        self.sample_df = pd.DataFrame({
            'Title': ['T-shirt 1', 'Hoodie 2'],
            'Price': [168000.0, 484000.0],
            'Rating': [4.2, 3.8],
            'Colors': [2, 3],
            'Size': ['S', 'L'],
            'Gender': ['Men', 'Unisex'],
            'timestamp': ['2025-05-13 14:41:10', '2025-05-13 14:41:10']
        })
    
    @patch('pandas.DataFrame.to_csv')
    @patch('os.makedirs')
    def test_save_to_csv(self, mock_makedirs, mock_to_csv):
        """Test saving to CSV"""
        # Call the function
        result = save_to_csv(self.sample_df, "test_output.csv")
        
        # Assert that makedirs was called
        mock_makedirs.assert_called_once_with('', exist_ok=True)
        
        # Assert that to_csv was called with the correct path
        mock_to_csv.assert_called_once_with('test_output.csv', index=False)
        
        # Assert that the function returned True
        self.assertTrue(result)
        
        # Test with exception
        mock_to_csv.side_effect = Exception("Test exception")
        result = save_to_csv(self.sample_df, "test_output.csv")
        self.assertFalse(result)
    
    @patch('google.oauth2.service_account.Credentials.from_service_account_file')
    @patch('googleapiclient.discovery.build')
    @patch('os.path.exists')
    def test_save_to_google_sheets(self, mock_exists, mock_build, mock_credentials):
        """Test saving to Google Sheets"""
        # Setup mocks
        mock_exists.return_value = True
        mock_service = MagicMock()
        mock_build.return_value = mock_service
        
        # Setup sheets API mock
        mock_sheets = MagicMock()
        mock_values = MagicMock()
        mock_clear = MagicMock()
        mock_update = MagicMock()
        
        mock_service.spreadsheets.return_value.values.return_value.clear.return_value.execute.return_value = {}
        mock_service.spreadsheets.return_value.values.return_value.update.return_value.execute.return_value = {"updatedCells": 12}
        
        # Call the function
        result = save_to_google_sheets(self.sample_df, "test_sheet_id", "test_credentials.json")
        
        # Assert that the credentials were loaded
        mock_credentials.assert_called_once_with("test_credentials.json", scopes=['https://www.googleapis.com/auth/spreadsheets'])
        
        # Assert that the service was built
        mock_build.assert_called_once_with('sheets', 'v4', credentials=mock_credentials.return_value)
        
        # Assert that the function returned True
        self.assertTrue(result)
        
        # Test with credentials file not found
        mock_exists.return_value = False
        result = save_to_google_sheets(self.sample_df, "test_sheet_id", "test_credentials.json")
        self.assertFalse(result)
    
    @patch('sqlalchemy.create_engine')
    def test_save_to_postgresql(self, mock_create_engine):
        """Test saving to PostgreSQL"""
        # Setup mock
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # Mock the to_sql method on DataFrame
        with patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
            # Call the function
            result = save_to_postgresql(self.sample_df, "postgresql://user:pass@localhost/db")
            
            # Assert that create_engine was called with the connection string
            mock_create_engine.assert_called_once_with("postgresql://user:pass@localhost/db")
            
            # Assert that to_sql was called with the correct parameters
            mock_to_sql.assert_called_once_with('fashion_products', mock_engine, if_exists='replace', index=False)
            
            # Assert that the function returned True
            self.assertTrue(result)
            
            # Test with exception
            mock_to_sql.side_effect = Exception("Test exception")
            result = save_to_postgresql(self.sample_df, "postgresql://user:pass@localhost/db")
            self.assertFalse(result)
    
    def test_load_data_empty_df(self):
        """Test load_data with empty DataFrame"""
        # Test with None
        result = load_data(None)
        self.assertEqual(result, {"csv": False, "google_sheets": False, "postgresql": False})
        
        # Test with empty DataFrame
        result = load_data(pd.DataFrame())
        self.assertEqual(result, {"csv": False, "google_sheets": False, "postgresql": False})
    
    @patch('utils.load.save_to_csv')
    @patch('utils.load.save_to_google_sheets')
    @patch('utils.load.save_to_postgresql')
    def test_load_data(self, mock_postgresql, mock_google_sheets, mock_csv):
        """Test load_data with all repositories"""
        # Setup mocks
        mock_csv.return_value = True
        mock_google_sheets.return_value = True
        mock_postgresql.return_value = True
        
        # Call the function with all repositories
        result = load_data(
            self.sample_df, 
            output_csv_path="test.csv", 
            google_sheet_id="test_sheet_id", 
            postgresql_conn_string="postgresql://user:pass@localhost/db"
        )
        
        # Assert that each save function was called
        mock_csv.assert_called_once_with(self.sample_df, "test.csv")
        mock_google_sheets.assert_called_once_with(self.sample_df, "test_sheet_id")
        mock_postgresql.assert_called_once_with(self.sample_df, "postgresql://user:pass@localhost/db")
        
        # Assert the result dictionary
        self.assertEqual(result, {"csv": True, "google_sheets": True, "postgresql": True})
        
        # Test with just CSV
        mock_csv.reset_mock()
        mock_google_sheets.reset_mock()
        mock_postgresql.reset_mock()
        
        result = load_data(self.sample_df, output_csv_path="test.csv")
        
        mock_csv.assert_called_once_with(self.sample_df, "test.csv")
        mock_google_sheets.assert_not_called()
        mock_postgresql.assert_not_called()
        
        self.assertEqual(result, {"csv": True, "google_sheets": None, "postgresql": None})

if __name__ == '__main__':
    unittest.main()