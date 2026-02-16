with trips as (
    -- เปลี่ยนไปอ่านจาก int_trips แทน fct_trips เพื่อลด cost และ circular dependency check
    select * from {{ ref('int_trips') }}
),

vendors as (
    select distinct
        vendor_id,
        {{ get_vendor_data('vendor_id') }} as vendor_name
    from trips
)

select * from vendors