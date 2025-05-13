import pandas as pd
import numpy as np
import re

def transform_data(df):
    """
    Transform the raw scraped data according to the requirements:
    - Convert price from USD to IDR (exchange rate 16,000)
    - Clean rating values to extract the numerical rating
    - Clean colors values to extract just the number
    - Clean size values to remove the "Size: " prefix
    - Clean gender values to remove the "Gender: " prefix
    - Remove duplicates and null values
    - Remove invalid products like "Unknown Product"
    
    Args:
        df (pandas.DataFrame): Raw data scraped from the website
        
    Returns:
        pandas.DataFrame: Transformed data
    """
    try:
        # Make a copy to avoid modifying the original DataFrame
        transformed_df = df.copy()
        
        # Drop rows with null values
        transformed_df = transformed_df.dropna()
        
        # Remove rows with "Unknown Product" in the Title column
        transformed_df = transformed_df[transformed_df['Title'] != 'Unknown Product']
        
        # Transform Price column: convert USD to IDR
        def convert_price_to_idr(price):
            try:
                if price and isinstance(price, str):
                    # Extract the numerical value from the price string
                    match = re.search(r'\$(\d+(?:\.\d+)?)', price)
                    if match:
                        # Convert to IDR with exchange rate of 16,000
                        price_value = float(match.group(1))
                        return price_value * 16000
                    else:
                        return np.nan
                else:
                    return np.nan
            except Exception as e:
                print(f"Error converting price '{price}': {str(e)}")
                return np.nan
                
        transformed_df['Price'] = transformed_df['Price'].apply(convert_price_to_idr)
        
        # Transform Rating column: extract the numerical rating
        def extract_rating(rating):
            try:
                if rating and isinstance(rating, str):
                    # Extract the numerical value from the rating string
                    match = re.search(r'(\d+\.\d+)', rating)
                    if match:
                        return float(match.group(1))
                    else:
                        return np.nan
                else:
                    return np.nan
            except Exception as e:
                print(f"Error extracting rating '{rating}': {str(e)}")
                return np.nan
                
        transformed_df['Rating'] = transformed_df['Rating'].apply(extract_rating)
        
        # Transform Colors column: extract just the number
        def extract_colors(colors):
            try:
                if colors and isinstance(colors, str):
                    # Extract the numerical value from the colors string
                    match = re.search(r'(\d+)', colors)
                    if match:
                        return int(match.group(1))
                    else:
                        return np.nan
                else:
                    return np.nan
            except Exception as e:
                print(f"Error extracting colors '{colors}': {str(e)}")
                return np.nan
                
        transformed_df['Colors'] = transformed_df['Colors'].apply(extract_colors)
        
        # Transform Size column: remove the "Size: " prefix
        def clean_size(size):
            try:
                if size and isinstance(size, str):
                    return size.replace('Size: ', '')
                else:
                    return np.nan
            except Exception as e:
                print(f"Error cleaning size '{size}': {str(e)}")
                return np.nan
                
        transformed_df['Size'] = transformed_df['Size'].apply(clean_size)
        
        # Transform Gender column: remove the "Gender: " prefix
        def clean_gender(gender):
            try:
                if gender and isinstance(gender, str):
                    return gender.replace('Gender: ', '')
                else:
                    return np.nan
            except Exception as e:
                print(f"Error cleaning gender '{gender}': {str(e)}")
                return np.nan
                
        transformed_df['Gender'] = transformed_df['Gender'].apply(clean_gender)
        
        # Drop any rows with NaN values after transformation
        transformed_df = transformed_df.dropna()
        
        # Drop duplicates
        transformed_df = transformed_df.drop_duplicates()
        
        # Ensure data types are correctly set
        transformed_df['Rating'] = transformed_df['Rating'].astype(float)
        transformed_df['Colors'] = transformed_df['Colors'].astype(int)
        transformed_df['Price'] = transformed_df['Price'].astype(float)
        
        return transformed_df
        
    except Exception as e:
        print(f"Error in transform_data: {str(e)}")
        return None

if __name__ == "__main__":
    # Test the transformation function
    try:
        # Load the raw data
        raw_df = pd.read_csv("raw_products.csv")
        
        # Transform the data
        transformed_df = transform_data(raw_df)
        
        if transformed_df is not None:
            # Print the first few rows
            print(transformed_df.head())
            
            # Display data types
            print("\nData Types:")
            print(transformed_df.dtypes)
            
            # Save the transformed data
            transformed_df.to_csv("transformed_products.csv", index=False)
            print("\nTransformed data saved to 'transformed_products.csv'")
        else:
            print("Transformation failed.")
    except Exception as e:
        print(f"Error testing transform_data: {str(e)}")