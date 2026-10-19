{{
    config(
        materialized='table',
        unique_key='store_id'
    )
}}

with raw_store as (
    select * from {{ source('raw', 'store') }}
),

selected_columns_store as (
    select
        store_id,
        manager_staff_id,
        address_id,
        {{ debezium_ts_micros('last_update') }} as last_update,
        {{ debezium_ts_milies('__ts_ms') }} as cdc_capture_at,
        case
            when __op = 'd' or cast(__deleted as boolean) = true then true
            else false
        end as is_deleted
    from raw_store
),

deduplicate_store as (
    select *,
        row_number() over (
            partition by store_id
            order by cdc_capture_at desc, is_deleted desc
        ) as dup_rank
    from selected_columns_store
)

select
    store_id,
    manager_staff_id,
    address_id,
    last_update,
    cdc_capture_at,
    is_deleted
from deduplicate_store
where dup_rank = 1
