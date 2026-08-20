{{
    config(
        materialized='table',
        unique_key='rental_id'
    )
}}

with raw_rental as (
    select * from {{ source('raw', 'rental') }}
),

selected_columns_rental as (
    select 
        rental_id,
        customer_id,
        inventory_id,
        staff_id,
        {{ debezium_ts_micros('rental_date') }} as rental_date,
        {{ debezium_ts_micros('return_date') }} as return_date,
        {{ debezium_ts_micros('last_update') }} as last_update,
        {{ debezium_ts_milies('__ts_ms') }} as cdc_capture_at,
        __op,
        cast(__deleted as boolean) as __deleted
    from raw_rental
),

filtered_rental as (
    select *
    from selected_columns_rental
    where (__deleted = false or __deleted is null)
      and (rental_date is null or rental_date <= {{ current_timestamp_iso() }})
      and (return_date is null or return_date >= rental_date)
),

deduplicate_rental as (
    select *,
        row_number() over (partition by rental_id order by cdc_capture_at desc) as dup_rank
    from filtered_rental
)

select 
    rental_id,
    customer_id,
    inventory_id,
    staff_id,
    rental_date,
    return_date,
    last_update,
    cdc_capture_at
from deduplicate_rental
where dup_rank = 1