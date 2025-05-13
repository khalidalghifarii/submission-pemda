import unittest
import pandas as pd
import os
import sys

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the transform functions
from utils.transform import clean_price, clean_rating, clean_colors, clean_size, clean_gender, transform_data

class TestTransform(unittest.TestCase):
    
    def test_clean_price(self):
        """Test price cleaning and conversion"""
        # Test with valid price
        self.assertEqual(clean_price("$100.00"), 1600000.0)
        self.assertEqual(clean_price("$50"), 800000.0)
        
        # Test with invalid price formats
        self.assertIsNone(clean_price("Price Unavailable"))
        self.assertIsNone(clean_price("Unavailable"))
        self.assertIsNone(clean_price(None))
        
    def test_clean_rating(self):
        """Test rating cleaning"""
        # Test with valid rating
        self.assertEqual(clean_rating("Rating: ⭐ 4.8 / 5"), 4.8)
        self.assertEqual(clean_rating("Rating: ⭐ 3 / 5"), 3.0)
        
        # Test with invalid rating formats
        self.assertIsNone(clean_rating("Rating: ⭐ Invalid Rating / 5"))
        self.assertIsNone(clean_rating("Rating: Not Rated"))
        self.assertIsNone(clean_rating(None))
        
    def test_clean_colors(self):
        """Test colors cleaning"""
        # Test with valid colors
        self.assertEqual(clean_colors("3 Colors"), 3)
        self.assertEqual(clean_colors("10 Colors"), 10)
        
        # Test with invalid colors formats
        self.assertIsNone(clean_colors("Colors"))
        self.assertIsNone(clean_colors(None))
        
    def test_clean_size(self):
        """Test size cleaning"""
        # Test with valid size
        self.assertEqual(clean_size("Size: M"), "M")
        self.assertEqual(clean_size("Size: XL"), "XL")
        
        # Test with invalid size formats
        self.assertIsNone(clean_size("Size"))
        self.assertIsNone(clean_size(None))
        
    def test_clean_gender(self):
        """Test gender cleaning"""
        # Test with valid gender
        self.assertEqual(clean_gender("Gender: Men"), "Men")
        self.assertEqual(clean_gender("Gender: Women"), "Women")
        self.assertEqual(clean_gender("Gender: Unisex"), "Unisex")
        
        # Test with invalid gender formats
        self.assertIsNone(clean_gender("Gender"))
        self.assertIsNone(clean_gender(None))
        
    def test_transform_data(self):
        """Test the complete transformation process"""
        # Create a sample DataFrame with raw data
        raw_data = pd.DataFrame({
            'Title': ['T-shirt 1', 'Unknown Product', 'Hoodie 3'],
            'Price': ['$10.50', 'Price Unavailable', '$30.25'],
            'Rating': ['Rating: ⭐ 4.2 / 5', 'Rating: ⭐ Invalid Rating / 5', 'Rating: ⭐ 3.8 / 5'],
            'Colors': ['2 Colors', '5 Colors', '3 Colors'],
            'Size': ['Size: S', 'Size: M', 'Size: L'],
            'Gender': ['Gender: Men', 'Gender: Women', 'Gender: Unisex'],
            'timestamp': ['2025-05-13 14:41:10', '2025-05-13 14:41:10', '2025-05-13 14:41:10']
        })
        
        # Transform the data
        transformed_data = transform_data(raw_data)
        
        # Check the shape (should have 2 rows after removing "Unknown Product")
        self.assertEqual(len(transformed_data), 2)
        
        # Check if price was converted correctly
        self.assertEqual(transformed_data.iloc[0]['Price'], 168000.0)
        self.assertEqual(transformed_data.iloc[1]['Price'], 484000.0)
        
        # Check if rating was cleaned correctly
        self.assertEqual(transformed_data.iloc[0]['Rating'], 4.2)
        self.assertEqual(transformed_data.iloc[1]['Rating'], 3.8)
        
        # Check if colors were cleaned correctly
        self.assertEqual(transformed_data.iloc[0]['Colors'], 2)
        self.assertEqual(transformed_data.iloc[1]['Colors'], 3)
        
        # Check if size was cleaned correctly
        self.assertEqual(transformed_data.iloc[0]['Size'], 'S')
        self.assertEqual(transformed_data.iloc[1]['Size'], 'L')
        
        # Check if gender was cleaned correctly
        self.assertEqual(transformed_data.iloc[0]['Gender'], 'Men')
        self.assertEqual(transformed_data.iloc[1]['Gender'], 'Unisex')
        
        # Test with empty DataFrame
        empty_df = pd.DataFrame()
        self.assertTrue(transform_data(empty_df).empty)
        
        # Test with None
        self.assertTrue(transform_data(None).empty)

if __name__ == '__main__':
    unittest.main()