"""@bruin

name: ingestion.trips
type: python
image: python:3.11
connection: gcp-default

depends:
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: append

columns:
  - name: VendorID
    type: integer
    description: TPEP/LPEP provider code
  - name: pickup_datetime
    type: timestamp
    description: Date and time when the meter was engaged
  - name: dropoff_datetime
    type: timestamp
    description: Date and time when the meter was disengaged
  - name: passenger_count
    type: float
    description: Number of passengers in the vehicle
  - name: trip_distance
    type: float
    description: Trip distance in miles
  - name: PULocationID
    type: integer
    description: TLC Taxi Zone pickup location ID
  - name: DOLocationID
    type: integer
    description: TLC Taxi Zone dropoff location ID
  - name: payment_type
    type: integer
    description: Payment type code
  - name: fare_amount
    type: float
    description: Fare amount
  - name: tip_amount
    type: float
    description: Tip amount
  - name: total_amount
    type: float
    description: Total amount charged to passengers
  - name: taxi_type
    type: string
    description: Type of taxi (yellow or green)
  - name: extracted_at
    type: timestamp
    description: Timestamp when the data was extracted

@bruin"""

import os
import json
import time
import pandas as pd
from datetime import datetime

MAX_DATA_DATE = datetime(2025, 11, 30)
MAX_RETRIES = 5
RETRY_BASE_WAIT = 30


def materialize():
    start_date = os.environ.get("BRUIN_START_DATE", "2022-01-01")
    end_date = os.environ.get("BRUIN_END_DATE", "2022-02-01")

    bruin_vars = json.loads(os.environ.get("BRUIN_VARS", '{}'))
    taxi_types = bruin_vars.get("taxi_types", ["yellow", "green"])

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    if end > MAX_DATA_DATE:
        print(f"Capping end date from {end_date} to {MAX_DATA_DATE.strftime('%Y-%m-%d')} (NYC TLC data limit)")
        end = MAX_DATA_DATE

    all_frames = []

    current = start
    while current < end:
        year = current.year
        month = current.month

        for taxi_type in taxi_types:
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month:02d}.parquet"

            if taxi_type == "yellow":
                pickup_col = "tpep_pickup_datetime"
                dropoff_col = "tpep_dropoff_datetime"
            else:
                pickup_col = "lpep_pickup_datetime"
                dropoff_col = "lpep_dropoff_datetime"

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    df = pd.read_parquet(url)

                    df = df.rename(columns={
                        pickup_col: "pickup_datetime",
                        dropoff_col: "dropoff_datetime",
                    })

                    keep_cols = [
                        "VendorID", "pickup_datetime", "dropoff_datetime",
                        "passenger_count", "trip_distance",
                        "PULocationID", "DOLocationID", "payment_type",
                        "fare_amount", "tip_amount", "total_amount",
                    ]
                    df = df[[c for c in keep_cols if c in df.columns]]

                    # Strip timezone info for clean loading
                    for col in df.select_dtypes(include=["datetimetz"]).columns:
                        df[col] = df[col].dt.tz_localize(None)

                    df["taxi_type"] = taxi_type
                    df["extracted_at"] = datetime.utcnow()

                    all_frames.append(df)
                    print(f"Fetched {len(df)} rows for {taxi_type} {year}-{month:02d}")
                    time.sleep(5)
                    break
                except Exception as e:
                    if ("403" in str(e) or "429" in str(e)) and attempt < MAX_RETRIES:
                        wait = RETRY_BASE_WAIT * attempt
                        print(f"Rate limited on {taxi_type} {year}-{month:02d}, waiting {wait}s (attempt {attempt}/{MAX_RETRIES})...")
                        time.sleep(wait)
                    else:
                        print(f"Skipping {taxi_type} {year}-{month:02d}: {e}")
                        break

        print(f"--- Finished {year}-{month:02d}, cooling down 10s ---")
        time.sleep(10)

        if month == 12:
            current = current.replace(year=year + 1, month=1)
        else:
            current = current.replace(month=month + 1)

    if all_frames:
        result = pd.concat(all_frames, ignore_index=True)

        # Ensure integer columns stay as nullable integers (not float from NaN)
        for col in ["VendorID", "PULocationID", "DOLocationID", "payment_type"]:
            if col in result.columns:
                result[col] = result[col].astype("Int64")

        print(f"Total rows to ingest: {len(result)}")
        return result
    else:
        print("No data fetched")
        return pd.DataFrame()
