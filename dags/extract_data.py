import logging
import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from google.cloud import storage
 
 # load enviroment variables
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] ='/Users/timolsoft/Desktop/maxi-project/single_infra/servicekey.json'
 
postgres_user = os.getenv('POSTGRES_USER')
postgres_db = os.getenv('POSTGRES_DB')
postgres_host = os.getenv('POSTGRES_HOST')
postgres_password =os.getenv('POSTGRES_PASSWORD')
postgres_port = 5432

bucket_name = 'tim-maxi-sales-bucket'

tables = ['customer', 'inventory', 'products', 'sale_transactions', 'sales_managers', 'stores']

def connect_2_postgres():
    try:
        conn = psycopg2.connect(
            dbname = postgres_db,
            user = postgres_user,
            password = postgres_password,
            host =postgres_host,
            port= postgres_port)
        logging.info("Connect to Postgres Database")
        return conn
    except Exception as e:
        logging.error(f"Error connecting to PostgresSOL : {e}")
        raise

def extract_data(conn, query):
    try:
        df = pd.read_sql_query(query,conn)
        return df
    except Exception as e:
        logging.error(f"Error executing  query: {query}. Error: {e}")
        return None

def format_data(df, file_name):
    try:
        df.to_parquet(file_name, index = False)
        return file_name
    except Exception as e:
        logging.error(f"Error formatting data: {e}" )
        return None
    
def upload_to_gcs(bucket_name,source_file_name, destination_blob_name):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        logging.info("fFile{source_file_name} uploaded to {destination_blob_name}.")
    except Exception as e:
        logging.error(f"Error uploading file to GCS: {e}")
        raise

def validate_data_quality(df):
    
    
        
        


        
        