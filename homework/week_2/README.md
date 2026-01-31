# Homework Week 2: Workflow Orchestration

## Question 1. Within the execution for Yellow Taxi data for the year 2020 and month 12: what is the uncompressed file size (i.e. the output file yellow_tripdata_2020-12.csv of the extract task)?

**Command:** using [text](kestra/flows/09_gcp_taxi_scheduled.yaml) in Kestra to find the answer in GCS

**Answer:** 134.5 MiB

## Question 2. What is the rendered value of the variable file when the inputs taxi is set to green, year is set to 2020, and month is set to 04 during execution?

**Command:** using [text](kestra/flows/04_postgres_taxi.yaml) in Kestra to figure it out.

**Answer:** green_tripdata_2020-04.csv


## Question 3. How many rows are there for the Yellow Taxi data for all CSV files in the year 2020?

**SQL Query (BigQuery):**
```sql
SELECT DISTINCT COUNT(*)
FROM `yellow_tripdata`
WHERE filename LIKE '%2020%';
```
**Answer:** 24,648,499

## Question 4. How many rows are there for the Green Taxi data for all CSV files in the year 2020?

**SQL Query (BigQuery):**
```sql
SELECT DISTINCT COUNT(*)
FROM `green_tripdata`
WHERE filename LIKE '%2020%';
```
**Answer:** 1,734,051

## Question 5. How many rows are there for the Yellow Taxi data for the March 2021 CSV file?

**Command:** using [text](kestra/flows/09_gcp_taxi_scheduled.yaml) in Kestra to find the answer in GCS

**Answer:** 1,925,152

## Question 6. How would you configure the timezone to New York in a Schedule trigger?

**Command:** using [text](kestra/flows/05_postgres_taxi_scheduled.yaml) in Kestra to figure it out with the document saying The time zone identifier (i.e. the second column in the Wikipedia table) to use for evaluating the cron expression. Default value is the server default zone ID.

**Answer:** Add a timezone property set to America/New_York in the Schedule trigger configuration
