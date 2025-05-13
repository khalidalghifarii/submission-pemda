import pandas as pd
import os
import json
from sqlalchemy import create_engine
from googleapiclient.discovery import build
from google.oauth2 import service_account

def save_to_csv(df, output_path="products.csv"):
    """
    Save DataFrame to CSV file
    
    Args:
        df (pandas.DataFrame): DataFrame to save
        output_path (str): Path to save the CSV file
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Ensure directory exists if needed
        dir_name = os.path.dirname(output_path)
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)
        
        df.to_csv(output_path, index=False)
        print(f"Data successfully saved to {output_path}")
        return True
    
    except Exception as e:
        print(f"Error saving data to CSV: {str(e)}")
        return False

def save_to_google_sheets(df, sheet_id, credentials_path="google-sheets-api.json"):
    """
    Save DataFrame to Google Sheets
    
    Args:
        df (pandas.DataFrame): DataFrame to save
        sheet_id (str): Google Sheets ID
        credentials_path (str): Path to Google Sheets API credentials
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Check if credentials file exists
        if not os.path.exists(credentials_path):
            print(f"Error: Google Sheets API credentials file not found at {credentials_path}")
            return False
        
        # Load credentials
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path, 
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        
        # Build the service
        service = build('sheets', 'v4', credentials=credentials)
        
        # Convert DataFrame to values list
        values = [df.columns.tolist()]  # Header row
        values.extend(df.values.tolist())  # Data rows
        
        # Prepare the request body
        body = {
            'values': values
        }
        
        # Clear the existing sheet first
        try:
            service.spreadsheets().values().clear(
                spreadsheetId=sheet_id,
                range='Sheet1',
                body={}
            ).execute()
            print("Existing sheet data cleared")
        except Exception as e:
            print(f"Warning: Could not clear existing sheet: {str(e)}")
        
        # Update the sheet with new values
        result = service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range='Sheet1',
            valueInputOption='RAW',
            body=body
        ).execute()
        
        print(f"Data successfully saved to Google Sheets. {result.get('updatedCells')} cells updated.")
        return True
    
    except Exception as e:
        print(f"Error saving data to Google Sheets: {str(e)}")
        return False

def save_to_postgresql(df, connection_string):
    """
    Save DataFrame to PostgreSQL database
    
    Args:
        df (pandas.DataFrame): DataFrame to save
        connection_string (str): Database connection string
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Create SQLAlchemy engine
        engine = create_engine(connection_string)
        
        # Save DataFrame to PostgreSQL
        df.to_sql('fashion_products', engine, if_exists='replace', index=False)
        
        print("Data successfully saved to PostgreSQL database")
        return True
    
    except Exception as e:
        print(f"Error saving data to PostgreSQL: {str(e)}")
        return False

def load_data(df, output_csv_path="products.csv", google_sheet_id=None, postgresql_conn_string=None):
    """
    Load transformed data to different data repositories
    
    Args:
        df (pandas.DataFrame): DataFrame to load
        output_csv_path (str): Path to save the CSV file
        google_sheet_id (str): Google Sheets ID
        postgresql_conn_string (str): PostgreSQL connection string
        
    Returns:
        dict: Dictionary with status for each repository
    """
    if df is None or df.empty:
        print("Error: Cannot load empty DataFrame")
        return {"csv": False, "google_sheets": False, "postgresql": False}
    
    results = {}
    
    # Save to CSV (always)
    results["csv"] = save_to_csv(df, output_csv_path)
    
    # Save to Google Sheets if ID is provided
    if google_sheet_id:
        results["google_sheets"] = save_to_google_sheets(df, google_sheet_id)
    else:
        results["google_sheets"] = None
        print("Google Sheets ID not provided, skipping")
    
    # Save to PostgreSQL if connection string is provided
    if postgresql_conn_string:
        results["postgresql"] = save_to_postgresql(df, postgresql_conn_string)
    else:
        results["postgresql"] = None
        print("PostgreSQL connection string not provided, skipping")
    
    return results

if __name__ == "__main__":
    # Test the loading functions
    try:
        print("Testing load.py...")
        
        # Load sample transformed data
        try:
            df = pd.read_csv("transformed_products.csv")
            
            # Test saving to CSV
            save_to_csv(df, "test_products.csv")
            
            # Note: Tests for Google Sheets and PostgreSQL would be here
            # but they require credentials/connection strings
            print("CSV loading test completed successfully.")
            
        except FileNotFoundError:
            print("Error: transformed_products.csv not found. Run transform.py first.")
        except Exception as e:
            print(f"Error during testing: {str(e)}")
    except Exception as e:
        print(f"Unexpected error in main: {str(e)}")