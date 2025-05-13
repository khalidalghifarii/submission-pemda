import os
import sys
import argparse
from datetime import datetime

# Add the utils directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import ETL components
from utils.extract import scrape_main
from utils.transform import transform_data
from utils.load import load_data

def run_etl_pipeline(
    max_pages=50,
    output_csv="products.csv",
    google_sheet_id=None,
    postgresql_conn_string=None
):
    """
    Run the complete ETL pipeline
    
    Args:
        max_pages (int): Maximum number of pages to scrape
        output_csv (str): Path to save the CSV file
        google_sheet_id (str): Google Sheets ID
        postgresql_conn_string (str): PostgreSQL connection string
        
    Returns:
        bool: True if successful, False otherwise
    """
    start_time = datetime.now()
    print(f"=== ETL Pipeline started at {start_time} ===")
    
    try:
        # Extract data
        print("\n=== EXTRACT PHASE ===")
        raw_data = scrape_main(max_pages=max_pages)
        if raw_data is None or raw_data.empty:
            print("Error: No data extracted. Pipeline terminated.")
            return False
        
        # Save raw data for debugging
        raw_data.to_csv("raw_products.csv", index=False)
        print(f"Raw data saved to raw_products.csv. Shape: {raw_data.shape}")
        
        # Transform data
        print("\n=== TRANSFORM PHASE ===")
        transformed_data = transform_data(raw_data)
        if transformed_data.empty:
            print("Error: Data transformation failed. Pipeline terminated.")
            return False
            
        # Save transformed data for debugging
        transformed_data.to_csv("transformed_products.csv", index=False)
        print(f"Transformed data saved to transformed_products.csv. Shape: {transformed_data.shape}")
        
        # Load data
        print("\n=== LOAD PHASE ===")
        load_results = load_data(
            transformed_data,
            output_csv_path=output_csv,
            google_sheet_id=google_sheet_id,
            postgresql_conn_string=postgresql_conn_string
        )
        
        # Print loading results
        print("\n=== LOAD RESULTS ===")
        for repo, status in load_results.items():
            if status is None:
                print(f"{repo}: Skipped")
            elif status:
                print(f"{repo}: Success")
            else:
                print(f"{repo}: Failed")
        
        end_time = datetime.now()
        duration = end_time - start_time
        print(f"\n=== ETL Pipeline completed at {end_time} (Duration: {duration}) ===")
        
        return all(status for status in load_results.values() if status is not None)
    
    except Exception as e:
        print(f"Error in ETL pipeline: {str(e)}")
        end_time = datetime.now()
        print(f"Pipeline terminated with error at {end_time}")
        return False

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Fashion Studio ETL Pipeline")
    parser.add_argument("--max-pages", type=int, default=50, help="Maximum number of pages to scrape")
    parser.add_argument("--output-csv", type=str, default="products.csv", help="Path to save the CSV file")
    parser.add_argument("--google-sheet-id", type=str, help="Google Sheets ID")
    parser.add_argument("--postgresql-conn", type=str, help="PostgreSQL connection string")
    
    args = parser.parse_args()
    
    # Run the ETL pipeline
    success = run_etl_pipeline(
        max_pages=args.max_pages,
        output_csv=args.output_csv,
        google_sheet_id=args.google_sheet_id,
        postgresql_conn_string=args.postgresql_conn
    )
    
    # Exit with appropriate status code
    sys.exit(0 if success else 1)