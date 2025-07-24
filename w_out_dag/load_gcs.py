import os
import pandas as pd
from google.cloud import storage

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/apple/Desktop/maxi-project/cloud_infra/credentials.json"
GCS_BUCKET_NAME = 'maxichistore'


def loadcsv_gcs(source_file):
    try:
        """Load data from Google Cloud Storage to a DataFrame"""
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(GCS_BUCKET_NAME)
        blob = bucket.blob('sales/sales_manager.parquet')
        with open(source_file, 'rb') as file:
            blob.upload_from_file(file, content_type='application/octet-stream')
        
        return source_file
    except Exception as e:
        print(f' error due to {e}')

loadcsv_gcs('/Users/apple/Desktop/maxi-project/snowflakes/sales_managers.csv')