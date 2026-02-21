#!/usr/bin/env python3
"""
Standalone script to load NYC TLC taxi data into BigQuery month by month.
Bypasses dlt/ingestr to avoid memory and schema issues with 315M+ rows.

Usage:
    uv run --with pandas --with pyarrow --with google-cloud-bigquery load_trips.py

Resumes from where it left off by checking a local progress file.
"""

import os
import sys
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

# Configuration
PROJECT_ID = "bruin-zoomcamp-2026"
DATASET_ID = "ingestion"
TABLE_ID = "trips"
FULL_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

START_YEAR, START_MONTH = 2019, 1
END_YEAR, END_MONTH = 2025, 11  # inclusive

TAXI_TYPES = ["yellow", "green"]

MAX_RETRIES = 5
RETRY_BASE_WAIT = 30
COOLDOWN_BETWEEN_FILES = 5
COOLDOWN_BETWEEN_MONTHS = 10

PROGRESS_FILE = Path(__file__).parent / ".load_progress.json"

# Column mapping: source camelCase -> BigQuery snake_case
COLUMN_RENAME = {
    "VendorID": "vendor_id",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
}

KEEP_COLS_SOURCE = [
    "VendorID", "pickup_datetime", "dropoff_datetime",
    "passenger_count", "trip_distance",
    "PULocationID", "DOLocationID", "payment_type",
    "fare_amount", "tip_amount", "total_amount",
]

INT_COLS = ["vendor_id", "pu_location_id", "do_location_id", "payment_type"]


def load_progress() -> set:
    """Load set of completed month keys like 'yellow-2019-01'."""
    if PROGRESS_FILE.exists():
        return set(json.loads(PROGRESS_FILE.read_text()))
    return set()


def save_progress(completed: set):
    PROGRESS_FILE.write_text(json.dumps(sorted(completed)))


def fetch_and_upload_month(client: bigquery.Client, year: int, month: int,
                           taxi_type: str, completed: set) -> int:
    """Download one month of data and upload to BigQuery. Returns row count."""
    key = f"{taxi_type}-{year}-{month:02d}"
    if key in completed:
        print(f"  SKIP {key} (already loaded)")
        return 0

    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month:02d}.parquet"

    if taxi_type == "yellow":
        pickup_col, dropoff_col = "tpep_pickup_datetime", "tpep_dropoff_datetime"
    else:
        pickup_col, dropoff_col = "lpep_pickup_datetime", "lpep_dropoff_datetime"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"  Downloading {key}...", end=" ", flush=True)
            df = pd.read_parquet(url)
            print(f"{len(df)} rows", flush=True)

            # Rename datetime columns
            df = df.rename(columns={pickup_col: "pickup_datetime", dropoff_col: "dropoff_datetime"})

            # Keep only needed columns
            df = df[[c for c in KEEP_COLS_SOURCE if c in df.columns]]

            # Rename camelCase to snake_case
            df = df.rename(columns=COLUMN_RENAME)

            # Strip timezone info
            for col in df.select_dtypes(include=["datetimetz"]).columns:
                df[col] = df[col].dt.tz_localize(None)

            # Add metadata columns
            df["taxi_type"] = taxi_type
            df["extracted_at"] = datetime.now(timezone.utc).replace(tzinfo=None)

            # Fix integer columns (NaN -> nullable Int64, then to float for BQ compatibility)
            # BigQuery INTEGER columns accept int values; NaN rows will be NULL
            for col in INT_COLS:
                if col in df.columns:
                    df[col] = df[col].astype("Int64")

            # Upload to BigQuery
            print(f"  Uploading {key} ({len(df)} rows)...", end=" ", flush=True)
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                # Let BigQuery auto-detect schema matching since table exists
            )

            job = client.load_table_from_dataframe(df, FULL_TABLE_ID, job_config=job_config)
            job.result()  # Wait for completion

            row_count = len(df)
            print(f"OK ({row_count} rows uploaded)", flush=True)

            # Free memory
            del df

            # Mark as completed
            completed.add(key)
            save_progress(completed)

            time.sleep(COOLDOWN_BETWEEN_FILES)
            return row_count

        except Exception as e:
            if ("403" in str(e) or "429" in str(e)) and attempt < MAX_RETRIES:
                wait = RETRY_BASE_WAIT * attempt
                print(f"\n  Rate limited on {key}, waiting {wait}s (attempt {attempt}/{MAX_RETRIES})...")
                time.sleep(wait)
            else:
                print(f"\n  FAILED {key}: {e}")
                if attempt == MAX_RETRIES:
                    print(f"  Giving up on {key} after {MAX_RETRIES} attempts")
                break

    return 0


def main():
    print(f"=== NYC TLC Taxi Data Loader ===")
    print(f"Target: {FULL_TABLE_ID}")
    print(f"Range: {START_YEAR}-{START_MONTH:02d} to {END_YEAR}-{END_MONTH:02d}")
    print()

    client = bigquery.Client(project=PROJECT_ID)

    # Check current table row count
    table = client.get_table(FULL_TABLE_ID)
    print(f"Current table rows: {table.num_rows}")
    print()

    completed = load_progress()
    if completed:
        print(f"Resuming: {len(completed)} files already loaded")
        print()

    total_rows = 0
    total_files = 0

    year, month = START_YEAR, START_MONTH
    while (year, month) <= (END_YEAR, END_MONTH):
        print(f"--- {year}-{month:02d} ---")

        for taxi_type in TAXI_TYPES:
            rows = fetch_and_upload_month(client, year, month, taxi_type, completed)
            total_rows += rows
            if rows > 0:
                total_files += 1

        print(f"  Month done. Running total: {total_rows:,} rows from {total_files} files")
        time.sleep(COOLDOWN_BETWEEN_MONTHS)

        # Advance to next month
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1

    # Final count
    table = client.get_table(FULL_TABLE_ID)
    print()
    print(f"=== DONE ===")
    print(f"Uploaded {total_rows:,} rows from {total_files} files this session")
    print(f"Total table rows: {table.num_rows:,}")


if __name__ == "__main__":
    main()
