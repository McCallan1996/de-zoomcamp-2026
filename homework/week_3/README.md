# Homework Week 3: Data Warehousing & BigQuery

## Question 1. Counting records: What is count of records for the 2024 Yellow Taxi Data?

**SQL Query (BigQuery):**
```sql
SELECT count(*)
FROM `yellow_tripdata_non_partitioned`;
```
**Answer:** 20,332,093

## Question 2. Data read estimation: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables. What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

**SQL Query (BigQuery):**
```sql
SELECT
COUNT(DISTINCT PULocationID)
FROM `external_yellow_tripdata`;

SELECT
COUNT(DISTINCT PULocationID)
FROM `yellow_tripdata_non_partitioned`;
```
**Answer:** 0 MB for the External Table and 155.12 MB for the Materialized Table

## Question 3. Understanding columnar storage: Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?

**SQL Query (BigQuery):**
```sql
SELECT 
  PULocationID
FROM `yellow_tripdata_non_partitioned`;

SELECT 
  PULocationID,
  DOLocationID
FROM `yellow_tripdata_non_partitioned`;
```
**Answer:** BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

## Question 4. Counting zero fare trips: How many records have a fare_amount of 0?

**SQL Query (BigQuery):**
```sql
SELECT 
  COUNT(fare_amount)
FROM `yellow_tripdata_non_partitioned`
WHERE fare_amount = 0;
```
**Answer:** 8,333

## Question 5. Partitioning and clustering: What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)

**SQL Query (BigQuery):**
```sql
CREATE OR REPLACE TABLE `yellow_tripdata_partitioned_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `external_yellow_tripdata`;
```
**Answer:** Partition by tpep_dropoff_datetime and Cluster on VendorID

## Question 6. Partition benefits: Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive) Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values? Choose the answer which most closely matches.

**SQL Query (BigQuery):**
```sql
SELECT
  DISTINCT VendorID
FROM `yellow_tripdata_non_partitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

SELECT
  DISTINCT VendorID
FROM `yellow_tripdata_partitioned_clustered`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
```
**Answer:** 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

## Question 7. External table storage: Where is the data stored in the External Table you created?

**Answer:** GCP Bucket

## Question 8. Clustering best practices: It is best practice in Big Query to always cluster your data:

**Answer:** False

**Reason:** No need to cluster if 1. Small Dataset (< 1 GB), 2. Low Cardinality. If do so, it leads to Metadata Overhead and write cost.

## Question 9. Understanding table scans: No Points: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

**SQL Query (BigQuery):**
```sql
SELECT 
COUNT(*)
FROM `external_yellow_tripdata`;
```
**Answer:** This query will process 0 B when run.

**Reason:** BigQuery does not need to scan the actual table data to answer this specific query. Why?
1. Metadata Storage: BigQuery maintains metadata about every table separately from the actual data storage. This metadata includes the total row count of the table.
2. Optimization: When you execute a SELECT count(*) query without any WHERE clause or filters, BigQuery is smart enough to retrieve the row count directly from this metadata.
3. No Table Scan: Since it reads the value from metadata, it avoids scanning the underlying storage columns entirely. Consequently, the "bytes processed" metric remains at 0; however, if you adjust the query from SELECT count(*) to SELECT count(column_name), BigQuery would be forced to scan the actual data, and the bytes processed would no longer be zero.