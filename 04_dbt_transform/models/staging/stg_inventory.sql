{{
    config(
        materialized='table',
        unique_key='inventory_id'
    )
}}

with raw_inventory as (
    select * from {{ source('raw', 'inventory') }}
),

selected_columns_inventory as (
    select
        inventory_id,
        film_id,
        store_id,
        {{ debezium_ts_micros('last_update') }} as last_update,
        {{ debezium_ts_milies('__ts_ms') }} as cdc_capture_at,
        __op,
        cast(__deleted as boolean) as __deleted
    from raw_inventory
),

filtered_inventory as (
    select *
    from selected_columns_inventory
    where __deleted = false or __deleted is null
),

deduplicate_inventory as (
    select *,
        row_number() over (partition by inventory_id order by cdc_capture_at desc) as dup_rank
    from filtered_inventory
)

select
    inventory_id,
    film_id,
    store_id,
    last_update
from deduplicate_inventory
where dup_rank = 1
