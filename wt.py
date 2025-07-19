import logging
from datetime import datetime
import great_expectations as gx
from google.cloud import storage
import psycopg2 
import pandas as pd
import os

# Configuration
TABLES = ['stores', 'sale_transactions', 'inventory', 'customers', 'products', 'sales_managers']
GCS_BUCKET_NAME = 'maxichistore'

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] ="/Users/apple/Desktop/maxi-project/single_infra/credentials.json"


POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'chichi')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = 5432
POSTGRES_DB = os.getenv('POSTGRES_DB', 'sales_db')

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
        print(f"Error connecting to PostgreSQL: {e}")
        return None

def extract_data(conn, query):
    try:
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        print(f"Error executing query: {e}")
        return None

def format_data_to_parquet(df, filename="customers.parquet"):
    try:
        df.to_parquet(filename, index=False)
        return filename
    except Exception as e:
        print(f"Error formatting data to: {e}")
        return None
    
def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        
        # Upload the file
        blob.upload_from_filename(source_file_name)
        
        print(f"File {source_file_name} uploaded to {destination_blob_name}.")
    except Exception as e:
        print(f"Error uploading to GCS: {e}")



GCS_FILE_NAME = "customers.parquet"
if __name__ == "__main__":
    # Define your query
    query = "SELECT * FROM customers;"

    # Extract data
    conn = connect_to_postgres()
    if conn:
        df = extract_data(conn, query)

        if df is not None:
            # Format data to Parquet
            parquet_file = format_data_to_parquet(df, GCS_FILE_NAME)

            if parquet_file:
                # Upload to GCS
                upload_to_gcs(GCS_BUCKET_NAME, parquet_file, f"sales/{GCS_FILE_NAME}")

        conn.close()