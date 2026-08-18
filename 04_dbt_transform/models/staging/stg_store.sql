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
        __op,
        cast(__deleted as boolean) as __deleted
    from raw_store
),

filtered_store as (
    select *
    from selected_columns_store
    where __deleted = false or __deleted is null
),

deduplicate_store as (
    select *,
        row_number() over (partition by store_id order by cdc_capture_at desc) as dup_rank
    from filtered_store
)

select
    store_id,
    manager_staff_id,
    address_id,
    last_update
from deduplicate_store
where dup_rank = 1
