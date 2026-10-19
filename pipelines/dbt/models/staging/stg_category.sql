{{
    config(
        materialized='table',
        unique_key='category_id'
    )
}}

with raw_category as (
    select * from {{ source('raw', 'category') }}
),

selected_columns_category as (
    select
        category_id,
        name as category_name,
        {{ debezium_ts_micros('last_update') }} as last_update,
        {{ debezium_ts_milies('__ts_ms') }} as cdc_capture_at,
        case
            when __op = 'd' or cast(__deleted as boolean) = true then true
            else false
        end as is_deleted
    from raw_category
),

deduplicate_category as (
    select *,
        row_number() over (
            partition by category_id
            order by cdc_capture_at desc, is_deleted desc
        ) as dup_rank
    from selected_columns_category
)

select
    category_id,
    category_name,
    last_update,
    cdc_capture_at,
    is_deleted
from deduplicate_category
where dup_rank = 1
