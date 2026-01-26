# Homework Week 1: Docker & SQL

## Question 1. Understanding Docker images

**Command:** `docker run -it --entrypoint bash python:3.13`

**Answer:** 25.3

## Question 2. Understanding Docker networking and docker-compose

**Answer:** db:5432 (or postgres:5432)

**Reasoning:**
- **Hostname:** Within the Docker network, containers communicate using their service name (`db`) or container name (`postgres`).
- **Port** They communicate internally via the container's internal port (`5432`).

## Question 3. Counting short trips

**SQL Query:**
```sql
SELECT count(*) 
FROM green_taxi_trips 
WHERE lpep_pickup_datetime >= '2025-11-01' 
AND lpep_pickup_datetime < '2025-12-01' 
AND trip_distance <= 1;
```
**Answer:** 8,007

## Question 4. Longest trip for each day

**SQL Query:**
```sql
SELECT 
    CAST(lpep_pickup_datetime AS DATE) as pickup_date, 
    MAX(trip_distance) as max_trip_distance 
FROM green_taxi_trips 
WHERE trip_distance < 100 
GROUP BY pickup_date 
ORDER BY max_trip_distance DESC
LIMIT 5;
```
**Answer:** 2025-11-14

## Question 5. Biggest pickup zone

**SQL Query:**
```sql
SELECT 
    zpu."Zone", 
    SUM(tpu.total_amount) AS total_amount 
FROM green_taxi_trips tpu 
JOIN zones zpu ON tpu."PULocationID" = zpu."LocationID" 
WHERE CAST(tpu.lpep_pickup_datetime AS DATE) = '2025-11-18' 
GROUP BY zpu."Zone"
ORDER BY total_amount DESC 
LIMIT 1;
```
**Answer:** East Harlem North