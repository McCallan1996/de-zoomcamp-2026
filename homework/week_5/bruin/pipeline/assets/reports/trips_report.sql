/* @bruin

name: reports.trips_report
type: bq.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_date
  time_granularity: date

columns:
  - name: taxi_type
    type: varchar
    description: Type of taxi (yellow or green)
    primary_key: true
  - name: pickup_date
    type: date
    description: Date of pickup
    primary_key: true
  - name: payment_type_name
    type: varchar
    description: Payment method name
    primary_key: true
  - name: total_trips
    type: bigint
    description: Total number of trips
    checks:
      - name: non_negative
  - name: total_passengers
    type: float
    description: Total number of passengers
  - name: avg_trip_distance
    type: float
    description: Average trip distance in miles
  - name: total_fare
    type: float
    description: Total fare amount
  - name: total_tips
    type: float
    description: Total tip amount
  - name: total_revenue
    type: float
    description: Total revenue (fare + tips + other)

@bruin */

SELECT
    taxi_type,
    CAST(pickup_datetime AS DATE) AS pickup_date,
    COALESCE(payment_type_name, 'unknown') AS payment_type_name,
    COUNT(*) AS total_trips,
    SUM(passenger_count) AS total_passengers,
    AVG(trip_distance) AS avg_trip_distance,
    SUM(fare_amount) AS total_fare,
    SUM(tip_amount) AS total_tips,
    SUM(total_amount) AS total_revenue
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY
    taxi_type,
    CAST(pickup_datetime AS DATE),
    COALESCE(payment_type_name, 'unknown')
