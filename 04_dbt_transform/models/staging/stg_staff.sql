{{
    config(
        materialized='table',
        unique_key='staff_id'
    )
}}

with raw_staff as (
    select * from {{ source('raw', 'staff') }}
),

selected_columns_staff as (
    select
        staff_id,
        first_name,
        last_name,
        address_id,
        email,
        store_id,
        cast(active as boolean) as is_active,
        username,
        {{ debezium_ts_micros('last_update') }} as last_update,
        {{ debezium_ts_milies('__ts_ms') }} as cdc_capture_at,
        __op,
        cast(__deleted as boolean) as __deleted
    from raw_staff
),

filtered_staff as (
    select *
    from selected_columns_staff
    where __deleted = false or __deleted is null
),

deduplicate_staff as (
    select *,
        row_number() over (partition by staff_id order by cdc_capture_at desc) as dup_rank
    from filtered_staff
)

select
    staff_id,
    first_name,
    last_name,
    address_id,
    email,
    store_id,
    is_active,
    username,
    last_update
from deduplicate_staff
where dup_rank = 1
