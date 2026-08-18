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
        __op,
        cast(__deleted as boolean) as __deleted
    from raw_category
),

filtered_category as (
    select *
    from selected_columns_category
    where __deleted = false or __deleted is null
),

deduplicate_category as (
    select *,
        row_number() over (partition by category_id order by cdc_capture_at desc) as dup_rank
    from filtered_category
)

select
    category_id,
    category_name,
    last_update
from deduplicate_category
where dup_rank = 1
