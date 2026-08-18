{{
    config(
        materialized='table',
        unique_key='address_id'
    )
}}

with raw_address as (
    select * from {{ source('raw', 'address') }}
),

selected_columns_address as (
    select
        address_id,
        address,
        address2,
        district,
        city_id,
        postal_code,
        phone,
        {{ debezium_ts_micros('last_update') }} as last_update,
        {{ debezium_ts_milies('__ts_ms') }} as cdc_capture_at,
        __op,
        cast(__deleted as boolean) as __deleted
    from raw_address
),

filtered_address as (
    select *
    from selected_columns_address
    where __deleted = false or __deleted is null
),

deduplicate_address as (
    select *,
        row_number() over (partition by address_id order by cdc_capture_at desc) as dup_rank
    from filtered_address
)

select
    address_id,
    address,
    address2,
    district,
    city_id,
    postal_code,
    phone,
    last_update
from deduplicate_address
where dup_rank = 1
