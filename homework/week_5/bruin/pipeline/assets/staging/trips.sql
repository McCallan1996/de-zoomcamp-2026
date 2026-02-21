/* @bruin

name: staging.trips
type: bq.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: trip_id
    type: varchar
    description: Unique trip identifier (hash of key fields)
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: pickup_datetime
    type: timestamp
    description: Pickup date and time
  - name: dropoff_datetime
    type: timestamp
    description: Dropoff date and time
  - name: taxi_type
    type: varchar
    description: Type of taxi (yellow or green)
  - name: passenger_count
    type: float
    description: Number of passengers
  - name: trip_distance
    type: float
    description: Trip distance in miles
    checks:
      - name: non_negative
  - name: payment_type
    type: integer
    description: Payment type code
  - name: payment_type_name
    type: varchar
    description: Payment type name from lookup
  - name: fare_amount
    type: float
    description: Fare amount in dollars
  - name: tip_amount
    type: float
    description: Tip amount in dollars
  - name: total_amount
    type: float
    description: Total trip amount
    checks:
      - name: non_negative

custom_checks:
  - name: no_duplicate_trips
    description: Ensure no duplicate trip IDs exist
    query: |
      SELECT COUNT(*) - COUNT(DISTINCT trip_id)
      FROM staging.trips
    value: 0

@bruin */

WITH filtered AS (
    SELECT
        t.*,
        p.payment_type_name
    FROM ingestion.trips t
    LEFT JOIN ingestion.payment_lookup p
        ON t.payment_type = p.payment_type_id
    WHERE t.pickup_datetime >= '{{ start_datetime }}'
      AND t.pickup_datetime < '{{ end_datetime }}'
      AND t.pickup_datetime IS NOT NULL
      AND t.trip_distance >= 0
      AND t.total_amount >= 0
),
with_hash AS (
    SELECT
        TO_HEX(MD5(
            COALESCE(CAST(vendor_id AS STRING), '') || '-' ||
            COALESCE(CAST(pickup_datetime AS STRING), '') || '-' ||
            COALESCE(CAST(dropoff_datetime AS STRING), '') || '-' ||
            COALESCE(CAST(pu_location_id AS STRING), '') || '-' ||
            COALESCE(CAST(do_location_id AS STRING), '') || '-' ||
            COALESCE(CAST(fare_amount AS STRING), '') || '-' ||
            COALESCE(CAST(tip_amount AS STRING), '') || '-' ||
            COALESCE(CAST(total_amount AS STRING), '') || '-' ||
            COALESCE(taxi_type, '')
        )) AS trip_id,
        pickup_datetime,
        dropoff_datetime,
        taxi_type,
        vendor_id,
        passenger_count,
        trip_distance,
        pu_location_id AS pickup_location_id,
        do_location_id AS dropoff_location_id,
        payment_type,
        payment_type_name,
        fare_amount,
        tip_amount,
        total_amount,
        extracted_at,
        ROW_NUMBER() OVER (
            PARTITION BY vendor_id, pickup_datetime, dropoff_datetime,
                         pu_location_id, do_location_id,
                         CAST(fare_amount AS STRING),
                         CAST(tip_amount AS STRING),
                         CAST(total_amount AS STRING),
                         taxi_type
            ORDER BY extracted_at DESC
        ) AS row_num
    FROM filtered
)
SELECT
    trip_id, pickup_datetime, dropoff_datetime, taxi_type, vendor_id,
    passenger_count, trip_distance, pickup_location_id, dropoff_location_id,
    payment_type, payment_type_name, fare_amount, tip_amount, total_amount, extracted_at
FROM with_hash
WHERE row_num = 1
