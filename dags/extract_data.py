import logging
import os
import psycopg2
from dotenv import load_dotenv
from google.cloud import storage
import pandas as pd

#load environment variables from .env file
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\hp\OneDrive - University of Stirling\Desktop\maxi-project\single_infra\servicekey.json'
load_dotenv()

postgres_user=os.getenv('POSTGRES_USER')
postgres_password=os.getenv('POSTGRES_PASSWORD')
postgres_port=5432
postgres_host=os.getenv('POSTGRES_HOST')
postgres_db=os.getenv('POSTGRES_DB')

bucket_name = 'maxi-sales-bucket92'
Tables=['stores', 'sale_transactions', 'products', 'inventory', 'customers', 'sales_managers']

def connect_to_postgres():
    try:
        conn= psycopg2.connect(
            user=postgres_user,
            password=postgres_password,
            host=postgres_host,
            port=postgres_port,
            database=postgres_db
            
        )
        logging.info("connected to postgresql database successfully")
        return conn
    except Exception as e:
        logging.error(f"Error connecting to PostgreSQL database: {e}")
        raise
    
def extract_data(conn, query):
    try:
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        return None
    
def format_data_to_parquet(df, file_name):
    try:
        df.to_parquet(file_name, index=False)
        return file_name
    except Exception as e:
        logging.error(f"Error formatting data: {e}")
        return None
    
def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(f"sales/{destination_blob_name}")
        blob.upload_from_filename(source_file_name)
        logging.info(f"file {source_file_name} uploaded to {destination_blob_name}")
    except Exception as e:
        logging.error(f"error uploading file to GCS: {e}")
        raise
    
def validate_data_quality(table_name:str, conn):
    try:
        primary_keys= {
        'stores': 'store_id',
        'sale_transactions': 'transaction_id',
        'products': 'product_id',
        'inventory': 'inventory_id',
        'customers': 'customer_id',
        'sales_managers': 'manager_id'
        }
        
        pk_column = primary_keys.get(table_name, None)
        validation = {'row_count': f"SELECT COUNT(*) FROM {table_name}",
                    'null_count': f"SELECT COUNT(*) FROM {table_name} WHERE {pk_column} is NULL",
                    'duplicate_count': f"SELECT COUNT(*) - COUNT(DISTINCT {pk_column}) FROM {table_name}"}
        results = {}
        for validation_name, query in validation.items():
            count = pd.read_sql_query(query, conn).iloc[0, 0]  # just the value
            results[validation_name] = count  # store in dictionary
            logging.info(f"{table_name} - {validation_name}: {count}")
        
        # Define validation rules
        validation_passed = True
        
        #check if table has data
        if results['row_count'] == 0:
            logging.error(f"{table_name}: Table is empty")
            validation_passed = False
            
        #check for null primary keys(assuming 'id' column exists)
        if results['null_count'] > 0:
            logging.warning(f"{table_name}: found {results['null_count']} null primary keys")
        
        #check for duplicates
        if results['duplicate_count'] > 0:
            logging.warning(f"{table_name}: found {results['duplicate_count']} duplicate records")
            
        if validation_passed:
            logging.info(f"{table_name}: Data quality validation passed")
            return {"status": "passed", "table": table_name, "metrics": results}
        else:
            raise ValueError(f"{table_name}: Data quality validation failed")
        
    except Exception as e:
        logging.error(f"validation failed for {table_name}: {str(e)}")
        raise

#Mainfunction to run the entire pipeline
# Make sure 'sales' folder exists
os.makedirs("sales", exist_ok=True)

if __name__ == "__main__":
    for table_name in Tables:
        # Define your query for each table
        query = f"SELECT * FROM {table_name};"

        # Connect to PostgreSQL
        conn = connect_to_postgres()
        if conn:
            # Extract data
            df = extract_data(conn, query)

            if df is not None:
                logging.info(f"✅ Data extracted for table: {table_name}")

                # Save as CSV
                csv_file = os.path.join("sales", f"{table_name}.csv")
                df.to_csv(csv_file, index=False)
                logging.info(f"📄 CSV saved: {csv_file}")

                # Upload CSV to GCS
                upload_to_gcs(bucket_name, csv_file, f"{table_name}.csv")
                logging.info(f"☁️ Uploaded {table_name}.csv to GCS")

                # Perform data quality validation
                validate_data_quality(table_name, conn)

            conn.close()
        else:
            logging.error(f"❌ Failed to connect to PostgreSQL for table: {table_name}")