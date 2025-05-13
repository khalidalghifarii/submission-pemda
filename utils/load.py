import pandas as pd
import os

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

def load_data(df, output_csv_path="products.csv", google_sheet_id=None, postgresql_conn_string=None):
    """
    Load transformed data to repositories
    
    Args:
        df (pandas.DataFrame): DataFrame to load
        output_csv_path (str): Path to save the CSV file
        google_sheet_id (str, optional): Google Sheets ID (not implemented)
        postgresql_conn_string (str, optional): PostgreSQL connection string (not implemented)
        
    Returns:
        dict: Dictionary with status for each repository
    """
    if df is None or df.empty:
        print("Error: Cannot load empty DataFrame")
        return {"csv": False, "google_sheets": False, "postgresql": False}
    
    results = {}
    
    # Save to CSV
    results["csv"] = save_to_csv(df, output_csv_path)
    
    # Report other repositories as None (skipped)
    results["google_sheets"] = None
    results["postgresql"] = None
    
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
            
            print("CSV loading test completed successfully.")
            
        except FileNotFoundError:
            print("Error: transformed_products.csv not found. Run transform.py first.")
        except Exception as e:
            print(f"Error during testing: {str(e)}")
    except Exception as e:
        print(f"Unexpected error in main: {str(e)}")