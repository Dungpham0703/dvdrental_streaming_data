{{
    config(
        materialized='table',
        unique_key='language_id'
    )
}}

with raw_language as (
    select * from {{ source('raw', 'language') }}
),

selected_columns_language as (
    select
        language_id,
        name as language_name,
        {{ debezium_ts_micros('last_update') }} as last_update,
        {{ debezium_ts_milies('__ts_ms') }} as cdc_capture_at,
        __op,
        cast(__deleted as boolean) as __deleted
    from raw_language
),

filtered_language as (
    select *
    from selected_columns_language
    where __deleted = false or __deleted is null
),

deduplicate_language as (
    select *,
        row_number() over (partition by language_id order by cdc_capture_at desc) as dup_rank
    from filtered_language
)

select
    language_id,
    language_name,
    last_update
from deduplicate_language
where dup_rank = 1
