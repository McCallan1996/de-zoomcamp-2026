import requests
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account

# --- CONFIGURATION ---
PROJECT_ID = "x"
BUCKET_NAME = "y"
CREDENTIALS_FILE = "z"

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_FILE)
storage_client = storage.Client(credentials=credentials, project=PROJECT_ID)
bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)


def create_bucket_if_not_exists():
    bucket = storage_client.bucket(BUCKET_NAME)
    if not bucket.exists():
        storage_client.create_bucket(bucket, location="US")
        print(f"Bucket {BUCKET_NAME} created.")
    else:
        print(f"Bucket {BUCKET_NAME} already exists.")


def delete_old_files(prefix):
    """Delete old files in GCS with given prefix."""
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = list(bucket.list_blobs(prefix=prefix))
    for blob in blobs:
        blob.delete()
    if blobs:
        print(f"Deleted {len(blobs)} old files from gs://{BUCKET_NAME}/{prefix}")


def upload_csv_gz(taxi_type, year, month):
    filename = f"{taxi_type}_tripdata_{year}-{month:02d}"
    url = f"{BASE_URL}/{taxi_type}/{filename}.csv.gz"
    blob_name = f"{taxi_type}/{filename}.csv.gz"

    print(f"Processing {filename}...")

    r = requests.get(url)
    if r.status_code != 200:
        print(f"  FAILED to download {url} (status {r.status_code})")
        return

    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(r.content, content_type="application/gzip")
    print(f"  Uploaded {blob_name} to GCS.")


def create_csv_external_table(taxi_type):
    table_id = f"{PROJECT_ID}.prod.{taxi_type}_tripdata"

    external_config = bigquery.ExternalConfig("CSV")
    external_config.source_uris = [f"gs://{BUCKET_NAME}/{taxi_type}/*.csv.gz"]
    external_config.autodetect = True
    csv_opts = external_config.csv_options
    csv_opts.skip_leading_rows = 1

    table = bigquery.Table(table_id)
    table.external_data_configuration = external_config

    bq_client.delete_table(table_id, not_found_ok=True)
    bq_client.create_table(table)
    print(f"Created external table {table_id} (CSV format)")


if __name__ == "__main__":
    # 1. Create Bucket
    create_bucket_if_not_exists()

    # 2. Create Dataset 'prod' in BigQuery
    dataset_id = f"{PROJECT_ID}.prod"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    bq_client.create_dataset(dataset, exists_ok=True)
    print(f"Dataset {dataset_id} ready.")

    # 3. Upload Green & Yellow taxi data (2019-2020)
    for taxi_type in ["green", "yellow"]:
        delete_old_files(f"{taxi_type}/")
        for year in [2019, 2020]:
            for month in range(1, 13):
                upload_csv_gz(taxi_type, year, month)
        create_csv_external_table(taxi_type)

    # 4. Upload FHV data (2019 only)
    delete_old_files("fhv/")
    for month in range(1, 13):
        upload_csv_gz("fhv", 2019, month)
    create_csv_external_table("fhv")

    print("\nAll Done! Setup complete.")
