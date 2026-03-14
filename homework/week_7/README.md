# Homework Week 7: Streaming with Kafka and PyFlink

## Setup

- Used Docker Compose to run Redpanda (Kafka-compatible broker), Flink (JobManager + TaskManager), and PostgreSQL.
- Configured PyFlink with JDBC and Kafka connectors via custom Dockerfile.
- Ingested Green Taxi Trip Data (October 2025) as JSON into the `green-trips` Redpanda topic.
- Submitted PyFlink streaming jobs for tumbling and session window aggregations, writing results to PostgreSQL.

## Question 1. Redpanda Version

Run `rpk version` inside the Redpanda container. What version of Redpanda are you getting?

**Answer:** v25.3.9

```bash
docker exec -it week_7-redpanda-1 rpk version
```

## Question 2. Sending Data to Redpanda

How much time did it take to send all the data?

**Answer:** 10 seconds


```bash
docker exec -it week_7-redpanda-1 rpk topic create green-trips
uv run src/producers/producer.py
```

## Question 3. Consumer - Trip Distance

How many trips have a distance greater than 5.0 km?

**Answer:** 8506

Wrote a Kafka consumer with `auto_offset_reset='earliest'` that reads all messages from `green-trips` and counts records where `trip_distance > 5.0`.

```bash
uv run src/consumers/consumer.py
```

## Question 4. Tumbling Window - Most Frequent Pickup Location

Using a 5-minute tumbling window, count the number of trips per `PULocationID`. Which `PULocationID` had the most trips in a single window?

**Answer:** 74

Submitted the tumbling window Flink job and queried PostgreSQL for the top result.

```bash
docker compose exec jobmanager ./bin/flink run -py /opt/src/job/tumbling_window_job.py --pyFiles /opt/src -d
# Query:
docker compose exec postgres psql -U postgres -c "SELECT PULocationID, num_trips FROM tumbling_window_results ORDER BY num_trips DESC LIMIT 5;"
```

## Question 5. Session Window - Longest Streak

Using a 5-minute session window on `PULocationID`, find the longest session. How many trips were in that session?

**Answer:** 81

Submitted the session window Flink job and queried PostgreSQL for the largest session.

```bash
docker compose exec jobmanager ./bin/flink run -py /opt/src/job/session_window_job.py --pyFiles /opt/src -d
# Query:
docker compose exec postgres psql -U postgres -c "SELECT PULocationID, num_trips FROM session_window_results ORDER BY num_trips DESC LIMIT 5;"
```

## Question 6. Tumbling Window - Largest Tip

Using a 1-hour tumbling window, compute the total `tip_amount` per hour. Which hour had the highest total tips?

**Answer:** 2025-10-16 18:00:00

Submitted the tips window Flink job and queried PostgreSQL for the top result.

```bash
docker compose exec jobmanager ./bin/flink run -py /opt/src/job/tips_window_job.py --pyFiles /opt/src -d
# Query:
docker compose exec postgres psql -U postgres -c "SELECT window_start, total_tips FROM tips_window_results ORDER BY total_tips DESC LIMIT 5;"
```
