{{
    config(
        materialized='table',
        unique_key='country_id'
    )
}}

with raw_country as (
    select * from {{ source('raw', 'country') }}
),

selected_columns_country as (
    select
        country_id,
        country as country_name,
        {{ debezium_ts_micros('last_update') }} as last_update,
        {{ debezium_ts_milies('__ts_ms') }} as cdc_capture_at,
        __op,
        cast(__deleted as boolean) as __deleted
    from raw_country
),

filtered_country as (
    select *
    from selected_columns_country
    where __deleted = false or __deleted is null
),

deduplicate_country as (
    select *,
        row_number() over (partition by country_id order by cdc_capture_at desc) as dup_rank
    from filtered_country
)

select
    country_id,
    country_name,
    last_update
from deduplicate_country
where dup_rank = 1
