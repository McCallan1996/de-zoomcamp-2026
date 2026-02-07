# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "google-cloud-bigquery",
# ]
# ///

from google.cloud import bigquery
import time

# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------
PROJECT_ID = "x"
DATASET_ID = "y"
BUCKET_NAME = "z"

# ---------------------------------------------------------
# CLIENT SETUP
# ---------------------------------------------------------
client = bigquery.Client(project=PROJECT_ID)

def create_dataset():
    dataset_id = f"{PROJECT_ID}.{DATASET_ID}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    try:
        client.get_dataset(dataset_id)
        print(f"Dataset {dataset_id} already exists.")
    except Exception:
        dataset = client.create_dataset(dataset, timeout=30)
        print(f"Created dataset {dataset.project}.{dataset.dataset_id}")

def create_external_table():
    sql = f"""
    CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.{DATASET_ID}.external_yellow_tripdata`
    OPTIONS (
      format = 'PARQUET',
      uris = ['gs://{BUCKET_NAME}/yellow_tripdata_2024-*.parquet']
    );
    """
    job = client.query(sql)
    job.result() 
    print("Created External Table: external_yellow_tripdata")

def create_regular_table():
    print("Creating Regular Table (this might take a moment)...")
    sql = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.yellow_tripdata_non_partitioned` AS
    SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.external_yellow_tripdata`;
    """
    job = client.query(sql)
    job.result()
    print("Created Regular Table: yellow_tripdata_non_partitioned")

# ---------------------------------------------------------
# EXECUTION
# ---------------------------------------------------------
if __name__ == "__main__":
    create_dataset()
    create_external_table()
    create_regular_table()
    print("All BigQuery tasks completed!")