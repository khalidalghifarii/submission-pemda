import pandas as pd
import re
import numpy as np

def clean_price(price):
    """
    Clean and convert price from USD to IDR
    
    Args:
        price (str): Price in string format (e.g., "$24.99" or "Price Unavailable")
        
    Returns:
        float or None: Converted price in IDR or None if price is unavailable
    """
    if pd.isna(price) or price == "Price Unavailable" or price == "Unavailable":
        return None
    
    # Extract numeric value from price string
    try:
        # Extract numeric part using regex
        match = re.search(r'\$?(\d+\.\d+|\d+)', price)
        if match:
            # Convert to float and multiply by exchange rate (16000 IDR per USD)
            price_usd = float(match.group(1))
            price_idr = price_usd * 16000
            return price_idr
        else:
            return None
    except Exception as e:
        print(f"Error cleaning price '{price}': {str(e)}")
        return None

def clean_rating(rating):
    """
    Clean rating data
    
    Args:
        rating (str): Rating in string format (e.g., "Rating: ⭐ 4.8 / 5" or "Rating: Not Rated")
        
    Returns:
        float or None: Numeric rating or None if rating is not available
    """
    if pd.isna(rating):
        return None
    
    try:
        # Extract rating using regex
        match = re.search(r'(\d+\.\d+|\d+)\s*\/\s*5', rating)
        if match:
            return float(match.group(1))
        elif "Invalid Rating" in rating or "Not Rated" in rating:
            return None
        else:
            return None
    except Exception as e:
        print(f"Error cleaning rating '{rating}': {str(e)}")
        return None

def clean_colors(colors):
    """
    Clean colors data by extracting number of colors
    
    Args:
        colors (str): Colors in string format (e.g., "3 Colors")
        
    Returns:
        int or None: Number of colors or None if data is not available
    """
    if pd.isna(colors):
        return None
    
    try:
        # Extract number of colors using regex
        match = re.search(r'(\d+)\s*Colors', colors)
        if match:
            return int(match.group(1))
        else:
            return None
    except Exception as e:
        print(f"Error cleaning colors '{colors}': {str(e)}")
        return None

def clean_size(size):
    """
    Clean size data by removing "Size: " prefix
    
    Args:
        size (str): Size in string format (e.g., "Size: M")
        
    Returns:
        str or None: Size value or None if data is not available
    """
    if pd.isna(size):
        return None
    
    try:
        # Remove "Size: " prefix
        match = re.search(r'Size:\s*(.+)', size)
        if match:
            return match.group(1).strip()
        else:
            return None
    except Exception as e:
        print(f"Error cleaning size '{size}': {str(e)}")
        return None

def clean_gender(gender):
    """
    Clean gender data by removing "Gender: " prefix
    
    Args:
        gender (str): Gender in string format (e.g., "Gender: Men")
        
    Returns:
        str or None: Gender value or None if data is not available
    """
    if pd.isna(gender):
        return None
    
    try:
        # Remove "Gender: " prefix
        match = re.search(r'Gender:\s*(.+)', gender)
        if match:
            return match.group(1).strip()
        else:
            return None
    except Exception as e:
        print(f"Error cleaning gender '{gender}': {str(e)}")
        return None

def transform_data(df):
    """
    Transform scraped data by cleaning and normalizing
    
    Args:
        df (pandas.DataFrame): DataFrame containing scraped data
        
    Returns:
        pandas.DataFrame: Cleaned and transformed DataFrame
    """
    try:
        # Verify the DataFrame is not empty
        if df is None or df.empty:
            print("Error: Input DataFrame is empty or None")
            return pd.DataFrame()
            
        # Create a copy to avoid modifying the original DataFrame
        df_clean = df.copy()
        
        print(f"Starting transformation. Initial shape: {df_clean.shape}")
        
        # Clean individual columns
        print("Cleaning Price column...")
        df_clean['Price'] = df_clean['Price'].apply(clean_price)
        
        print("Cleaning Rating column...")
        df_clean['Rating'] = df_clean['Rating'].apply(clean_rating)
        
        print("Cleaning Colors column...")
        df_clean['Colors'] = df_clean['Colors'].apply(clean_colors)
        
        print("Cleaning Size column...")
        df_clean['Size'] = df_clean['Size'].apply(clean_size)
        
        print("Cleaning Gender column...")
        df_clean['Gender'] = df_clean['Gender'].apply(clean_gender)
        
        # Remove invalid products (e.g., "Unknown Product")
        print("Removing invalid products...")
        df_clean = df_clean[df_clean['Title'] != "Unknown Product"]
        
        # Drop rows with null values in important columns
        print("Dropping null values...")
        df_clean = df_clean.dropna(subset=['Title', 'Price', 'Rating', 'Colors', 'Size', 'Gender'])
        
        # Drop duplicate rows based on all columns except timestamp
        print("Removing duplicates...")
        df_clean = df_clean.drop_duplicates(subset=['Title', 'Price', 'Rating', 'Colors', 'Size', 'Gender'])
        
        # Ensure correct data types
        print("Converting data types...")
        df_clean['Price'] = df_clean['Price'].astype(float)
        df_clean['Rating'] = df_clean['Rating'].astype(float)
        df_clean['Colors'] = df_clean['Colors'].astype(int)
        
        print(f"Transformation complete. Rows before: {len(df)}, Rows after: {len(df_clean)}")
        print(f"Final DataFrame info:\n{df_clean.dtypes}")
        
        return df_clean
    
    except Exception as e:
        print(f"Error in transform_data: {str(e)}")
        # Return empty DataFrame if transformation fails
        return pd.DataFrame()

if __name__ == "__main__":
    # Test the transformation
    try:
        print("Testing transform.py...")
        # Load sample data from CSV 
        try:
            sample_df = pd.read_csv("raw_products.csv")
            transformed_df = transform_data(sample_df)
            print(transformed_df.head())
            print(transformed_df.dtypes)
            # Save transformed data
            if not transformed_df.empty:
                transformed_df.to_csv("transformed_products.csv", index=False)
                print("Transformed data saved to transformed_products.csv")
        except FileNotFoundError:
            print("Error: raw_products.csv not found. Run extract.py first.")
        except Exception as e:
            print(f"Error during testing: {str(e)}")
    except Exception as e:
        print(f"Unexpected error in main: {str(e)}")