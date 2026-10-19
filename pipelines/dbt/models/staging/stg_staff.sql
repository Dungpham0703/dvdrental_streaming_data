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
        case
            when __op = 'd' or cast(__deleted as boolean) = true then true
            else false
        end as is_deleted
    from raw_staff
),

deduplicate_staff as (
    select *,
        row_number() over (
            partition by staff_id
            order by cdc_capture_at desc, is_deleted desc
        ) as dup_rank
    from selected_columns_staff
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
    last_update,
    cdc_capture_at,
    is_deleted
from deduplicate_staff
where dup_rank = 1
