{{
    config(
        materialized='table',
        unique_key='city_id'
    )
}}

with raw_city as (
    select * from {{ source('raw', 'city') }}
),

selected_columns_city as (
    select
        city_id,
        city as city_name,
        country_id,
        {{ debezium_ts_micros('last_update') }} as last_update,
        {{ debezium_ts_milies('__ts_ms') }} as cdc_capture_at,
        __op,
        cast(__deleted as boolean) as __deleted
    from raw_city
),

filtered_city as (
    select *
    from selected_columns_city
    where __deleted = false or __deleted is null
),

deduplicate_city as (
    select *,
        row_number() over (partition by city_id order by cdc_capture_at desc) as dup_rank
    from filtered_city
)

select
    city_id,
    city_name,
    country_id,
    last_update
from deduplicate_city
where dup_rank = 1
