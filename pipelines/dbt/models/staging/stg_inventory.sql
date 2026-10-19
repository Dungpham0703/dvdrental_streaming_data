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
        case
            when __op = 'd' or cast(__deleted as boolean) = true then true
            else false
        end as is_deleted
    from raw_inventory
),

deduplicate_inventory as (
    select *,
        row_number() over (
            partition by inventory_id
            order by cdc_capture_at desc, is_deleted desc
        ) as dup_rank
    from selected_columns_inventory
)

select
    inventory_id,
    film_id,
    store_id,
    last_update,
    cdc_capture_at,
    is_deleted
from deduplicate_inventory
where dup_rank = 1
