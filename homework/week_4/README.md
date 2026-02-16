# Homework Week 4: Analytics Engineering with dbt

## Setup

- Loaded Green and Yellow taxi data (2019-2020) and FHV data (2019) as CSV.gz from [DataTalksClub releases](https://github.com/DataTalksClub/nyc-tlc-data/releases) directly into GCS/BigQuery.
- BigQuery natively handles gzip-compressed CSVs and resolves type consistency issues that occur with Parquet (e.g., `airport_fee` inferred as integer in some months and float in others).
- dbt project: `taxi_rides_ny` with staging, intermediate, and marts layers.

## Question 1. dbt Lineage and Execution

Given a dbt project with the following structure:

```
models/
├── staging/
│   ├── stg_green_tripdata.sql
│   └── stg_yellow_tripdata.sql
└── intermediate/
    └── int_trips_unioned.sql (depends on stg_green_tripdata & stg_yellow_tripdata)
```

If you run `dbt run --select int_trips_unioned`, what models will be built?

**Answer:** `int_trips_unioned` only

**Reason:** The `--select` flag without the `+` prefix only builds the specified model itself. It does not automatically include upstream dependencies (`stg_green_tripdata`, `stg_yellow_tripdata`) — those must already exist. To include upstream models, you would use `dbt run --select +int_trips_unioned`.

## Question 2. dbt Tests

You've configured a generic test like this in your `schema.yml`:

```yaml
columns:
  - name: payment_type
    data_tests:
      - accepted_values:
          arguments:
            values: [1, 2, 3, 4, 5]
            quote: false
```

Your model `fct_trips` has been running successfully for months. A new value `6` now appears in the source data. What happens when you run `dbt test --select fct_trips`?

**Answer:** dbt will fail the test, returning a non-zero exit code

**Reason:** The `accepted_values` test checks that all values in the column are within the specified list. When value `6` appears, it violates the test constraint, causing a test failure with a non-zero exit code. dbt does not auto-update configurations or skip tests based on model changes.

## Question 3. Counting Records in fct_monthly_zone_revenue

After running the dbt project, query the `fct_monthly_zone_revenue` model. What is the count of records?

**dbt inline query:**
```sql
select count(*) as cnt
from {{ ref('fct_monthly_zone_revenue') }}
```

**Answer:** 12,184

## Question 4. Best Performing Zone for Green Taxis (2020)

Using the `fct_monthly_zone_revenue` table, find the pickup zone with the highest total revenue (`revenue_monthly_total_amount`) for Green taxi trips in 2020.

**dbt inline query:**
```sql
select
    pickup_zone,
    sum(revenue_monthly_total_amount) as total_rev
from {{ ref('fct_monthly_zone_revenue') }}
where service_type = 'Green'
  and revenue_month >= '2020-01-01'
  and revenue_month < '2021-01-01'
group by pickup_zone
order by total_rev desc
```

| pickup_zone           | total_rev      |
|-----------------------|----------------|
| East Harlem North     | 1,817,487.65   |
| East Harlem South     | 1,653,170.51   |
| Central Harlem        | 1,097,599.62   |
| Washington Heights S. | 879,941.80     |
| Morningside Heights   | 764,671.24     |

**Answer:** East Harlem North

## Question 5. Green Taxi Trip Counts (October 2019)

Using the `fct_monthly_zone_revenue` table, what is the total number of trips (`total_monthly_trips`) for Green taxis in October 2019?

**dbt inline query:**
```sql
select sum(total_monthly_trips) as total_trips
from {{ ref('fct_monthly_zone_revenue') }}
where service_type = 'Green'
  and revenue_month = '2019-10-01'
```

**Answer:** 384,624

## Question 6. Build a Staging Model for FHV Data

Create a staging model for the For-Hire Vehicle (FHV) trip data for 2019 with these requirements:
- Load the FHV trip data for 2019 into your data warehouse
- Create a staging model `stg_fhv_tripdata`
- Filter out records where `dispatching_base_num IS NULL`
- Rename fields to match project naming conventions (e.g., `PUlocationID` → `pickup_location_id`)

**Staging model** (`models/staging/stg_fhv_tripdata.sql`):
```sql
{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'fhv_tripdata') }}
),

renamed as (
    select
        cast(dispatching_base_num as string) as dispatching_base_num,
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropOff_datetime as timestamp) as dropoff_datetime,
        {{ safe_cast('PUlocationID', 'integer') }} as pickup_location_id,
        {{ safe_cast('DOlocationID', 'integer') }} as dropoff_location_id,
        cast(SR_Flag as string) as sr_flag,
        cast(Affiliated_base_number as string) as affiliated_base_number
    from source
    where dispatching_base_num is not null
)

select * from renamed

{% if var('is_test_run', default=true) %}
  limit 100
{% endif %}
```

**Count query (dbt inline):**
```sql
select count(*) as cnt
from {{ ref('stg_fhv_tripdata') }}
```

**Answer:** 43,244,693
