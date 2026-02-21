# Workshop 1: Data Ingestion with dlt

## Setup

- Built a custom REST API pipeline using [dlt](https://dlthub.com/) to load NYC Yellow Taxi trip data from a paginated JSON API into DuckDB.
- API endpoint: `https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api` — returns 1,000 records per page, stops on an empty page (10 pages total, 10,000 records).
- Pipeline uses `write_disposition="replace"` so re-runs are idempotent.
- Data is stored in `taxi-pipeline/taxi_pipeline.duckdb` under the `nyc_taxi.taxi_rows` table.
- Visualizations and queries built in a [Marimo](https://marimo.io/) notebook (`taxi-pipeline/analysis.py`).

## Question 1. What is the date range of the loaded dataset?

**Query:**
```sql
SELECT
    MIN(trip_pickup_date_time) AS earliest,
    MAX(trip_pickup_date_time) AS latest
FROM nyc_taxi.taxi_rows;
```

**Answer:** 2009-06-01 to 2009-07-01

## Question 2. What proportion of trips were paid by Credit Card?

**Query:**
```sql
SELECT
    payment_type,
    COUNT(*) AS trips,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
FROM nyc_taxi.taxi_rows
GROUP BY payment_type
ORDER BY trips DESC;
```

**Answer:** 26.66%


## Question 3. What is the total amount of tips?

**Query:**
```sql
SELECT ROUND(SUM(tip_amt), 2) AS total_tips
FROM nyc_taxi.taxi_rows;
```

**Answer:** $6,063.41
