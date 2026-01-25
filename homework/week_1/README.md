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