import os
from google.cloud import storage

# Set your real credentials and bucket name
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\hp\OneDrive - University of Stirling\Desktop\maxi-project\single_infra\servicekey.json"
GCS_BUCKET_NAME = 'maxi-sales-bucket92'
DESTINATION_BLOB_NAME = 'sales/sales_managers.csv'

def upload_csv_to_gcs(source_file_path):
    """Uploads a CSV file to GCS, overwriting any existing one with the same name"""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(DESTINATION_BLOB_NAME)

        # Delete old version if it exists
        if blob.exists():
            blob.delete()
            print(f"🗑️ Deleted old file: {DESTINATION_BLOB_NAME}")

        # Upload new CSV file
        blob.upload_from_filename(source_file_path, content_type='text/csv')
        print(f"✅ Uploaded {source_file_path} to gs://{GCS_BUCKET_NAME}/{DESTINATION_BLOB_NAME}")

    except Exception as e:
        print(f"❌ Error during upload: {e}")

# Local path to the CSV file
upload_csv_to_gcs(r"C:\Users\hp\OneDrive - University of Stirling\Desktop\maxi-project\sales_managers.csv")
