import logging
import os
import psycopg2
import pandas as pd
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

# Configuration
TABLES = ['stores', 'sale_transactions', 'inventory', 'customers', 'products', 'sales_managers']
GCS_BUCKET_NAME = 'maxi-sales-bucket001'

# Set Google Cloud credentials directly for local usage
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/apple/Desktop/maxi-project/cloud_infra/credentials.json"

POSTGRES_USER = 'postgres'  #os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = 'chinwa' #os.getenv('POSTGRES_PASSWORD')
POSTGRES_HOST =  'localhost'   #os.getenv('POSTGRES_HOST')
POSTGRES_PORT =   5434  #os.getenv('POSTGRES_PORT')
POSTGRES_DB =   'sales_db'  #os.getenv('POSTGRES_DB')

# Configure logging to output to the console
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Connect to PostgreSQL
def connect_to_postgres():
    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT
        )
        return conn
    except psycopg2.Error as e:
        logging.error(f"Error connecting to PostgreSQL: {e}")
        return None

# Extract data from PostgreSQL
def extract_data(conn, query):
    try:
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        return None

# Format data to CSV
def format_data_to_csv(df, filename):
    try:
        df.to_parquet(filename, index=False, date_format='%Y-%m-%d %H:%M:%S')
        return filename
    except Exception as e:
        logging.error(f"Error formatting data to CSV: {e}")
        return None

# Upload data to GCS
def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(f'sales_date/{destination_blob_name}')

        # Upload the file
        blob.upload_from_filename(source_file_name)
        logging.info(f"File {source_file_name} uploaded to {destination_blob_name}.")
    except Exception as e:
        logging.error(f"Error uploading to GCS: {e}")

# Custom validation logic
def validate_data_quality(table_name: str, conn):
    """ Custom data quality validation function """
    try:
        # Define primary key column for each table
        primary_keys = {
            'stores': 'store_id',
            'sale_transactions': 'transaction_id',
            'inventory': 'inventory_id',
            'customers': 'customer_id',
            'products': 'product_id',
            'sales_managers': 'manager_id'
        }

        pk_column = primary_keys.get(table_name, 'id')  # Default to 'id' if not found
        
        # Basic validation queries
        validations = {
            'row_count': f"SELECT COUNT(*) FROM {table_name}",
            'null_check': f"SELECT COUNT(*) FROM {table_name} WHERE {pk_column} IS NULL",
            'duplicate_check': f"SELECT COUNT(*) - COUNT(DISTINCT {pk_column}) FROM {table_name}"
        }

        results = {}
        for validation_name, query in validations.items():
            result = pd.read_sql(query, conn).iloc[0, 0]
            results[validation_name] = result
            logging.info(f"{table_name} - {validation_name}: {result}")

        # Define validation rules
        validation_passed = True
        
        # Check if table has data
        if results['row_count'] == 0:
            logging.error(f"{table_name}: Table is empty")
            validation_passed = False

        # Check for null primary keys (assuming 'id' column exists)
        if results['null_check'] > 0:
            logging.warning(f"{table_name}: Found {results['null_check']} null primary keys")

        # Check for duplicates
        if results['duplicate_check'] > 0:
            logging.warning(f"{table_name}: Found {results['duplicate_check']} duplicate records")

        if validation_passed:
            logging.info(f"{table_name}: Data quality validation passed")
            return {"status": "passed", "table": table_name, "metrics": results}
        else:
            raise ValueError(f"{table_name}: Data quality validation failed")
            
    except Exception as e:
        logging.error(f"Validation failed for {table_name}: {str(e)}")
        raise

# Main function to run the entire pipeline
if __name__ == "__main__":
    for table_name in TABLES:
        # Define your query for each table
        query = f"SELECT * FROM {table_name};"
        
        # Connect to PostgreSQL
        conn = connect_to_postgres()
        if conn:
            # Extract data from the table
            df = extract_data(conn, query)

            if df is not None:
                logging.info(f"Data extracted for table: {table_name}")

                # Format the data to CSV
                csv_file = format_data_to_csv(df, f"{table_name}.parquet")

                if csv_file:
                    # Upload the CSV file to GCS
                    upload_to_gcs(GCS_BUCKET_NAME, csv_file, f"{table_name}.parquet")

                # Perform data quality validation
                validate_data_quality(table_name, conn)
    
            conn.close()
        else:
            logging.error(f"Failed to connect to PostgreSQL for table: {table_name}")