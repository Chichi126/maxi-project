import logging
import os
import psycopg2
from dotenv import load_dotenv
from google.cloud import storage
import pandas as pd

# Load environment variables from .env file
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "service.json"

load_dotenv()

postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')
postgres_port = 5432
postgres_host = os.getenv('POSTGRES_HOST')
postgres_db = os.getenv('POSTGRES_DB')

bucket_name = 'maxisales_bucket'
Tables = ['stores', 'sales_transactions', 'products', 'inventory', 'customers', 'sales_managers']


def connect_to_postgres():
    try:
        conn = psycopg2.connect(
            user=postgres_user,
            password=postgres_password,
            host=postgres_host,
            port=postgres_port,
            database=postgres_db
        )
        logging.info("Connected to PostgreSQL database successfully")
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
        df.to_parquet(file_name, index=False, engine="pyarrow")
        return file_name
    except Exception as e:
        logging.error(f"Error formatting data to Parquet: {e}")
        return None


def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(f"sales/{destination_blob_name}")
        blob.upload_from_filename(source_file_name)
        logging.info(f"File {source_file_name} uploaded to {destination_blob_name}")
    except Exception as e:
        logging.error(f"Error uploading file to GCS: {e}")
        raise


def validate_data_quality(table_name: str, conn):
    try:
        primary_keys = {
            'stores': 'store_id',
            'sales_transactions': 'invoice_id',
            'products': 'product_id',
            'inventory': 'inventory_id',
            'customers': 'customer_id',
            'sales_managers': 'manager_id'
        }

        pk_column = primary_keys.get(table_name)
        results = {}

        # Always check row count
        row_count_query = f"SELECT COUNT(*) FROM {table_name}"
        results['row_count'] = pd.read_sql_query(row_count_query, conn).iloc[0, 0]
        logging.info(f"{table_name} - row_count: {results['row_count']}")

        # Only run null/duplicate checks if PK column exists in table
        if pk_column:
            table_cols = pd.read_sql_query(
                f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
                """, conn
            )['column_name'].tolist()

            if pk_column in table_cols:
                null_count_query = f"SELECT COUNT(*) FROM {table_name} WHERE {pk_column} IS NULL"
                results['null_count'] = pd.read_sql_query(null_count_query, conn).iloc[0, 0]
                logging.info(f"{table_name} - null_count: {results['null_count']}")

                duplicate_count_query = f"SELECT COUNT(*) - COUNT(DISTINCT {pk_column}) FROM {table_name}"
                results['duplicate_count'] = pd.read_sql_query(duplicate_count_query, conn).iloc[0, 0]
                logging.info(f"{table_name} - duplicate_count: {results['duplicate_count']}")
            else:
                logging.warning(f"PK column '{pk_column}' not found in {table_name}, skipping null/duplicate checks.")
        else:
            logging.warning(f"No PK mapping for {table_name}, skipping null/duplicate checks.")

        # Validation pass/fail
        validation_passed = results['row_count'] > 0
        if validation_passed:
            logging.info(f"{table_name}: Data quality validation passed")
            return {"status": "passed", "table": table_name, "metrics": results}
        else:
            raise ValueError(f"{table_name}: Table is empty")

    except Exception as e:
        logging.error(f"Validation failed for {table_name}: {str(e)}")
        raise


# Main function to run the entire pipeline
os.makedirs("sales_pq", exist_ok=True)

if __name__ == "__main__":
    for table_name in Tables:
        query = f"SELECT * FROM {table_name};"
        conn = connect_to_postgres()

        if conn:
            df = extract_data(conn, query)
            if df is not None:
                logging.info(f"✅ Data extracted for table: {table_name}")

                # Save as Parquet
                parquet_file = os.path.join("sales_pq", f"{table_name}.parquet")
                format_data_to_parquet(df, parquet_file)
                logging.info(f"📦 Parquet saved: {parquet_file}")

                # Upload Parquet to GCS
                upload_to_gcs(bucket_name, parquet_file, f"{table_name}.parquet")
                logging.info(f"☁️ Uploaded {table_name}.parquet to GCS")

                # Perform data quality validation
                validate_data_quality(table_name, conn)

            conn.close()
        else:
            logging.error(f"❌ Failed to connect to PostgreSQL for table: {table_name}")
