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
        case
            when __op = 'd' or cast(__deleted as boolean) = true then true
            else false
        end as is_deleted
    from raw_city
),

deduplicate_city as (
    select *,
        row_number() over (
            partition by city_id
            order by cdc_capture_at desc, is_deleted desc
        ) as dup_rank
    from selected_columns_city
)

select
    city_id,
    city_name,
    country_id,
    last_update,
    cdc_capture_at,
    is_deleted
from deduplicate_city
where dup_rank = 1
