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
