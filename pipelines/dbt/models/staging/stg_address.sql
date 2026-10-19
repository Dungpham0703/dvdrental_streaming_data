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
        case
            when __op = 'd' or cast(__deleted as boolean) = true then true
            else false
        end as is_deleted
    from raw_address
),

deduplicate_address as (
    select *,
        row_number() over (
            partition by address_id
            order by cdc_capture_at desc, is_deleted desc
        ) as dup_rank
    from selected_columns_address
)

select
    address_id,
    address,
    address2,
    district,
    city_id,
    postal_code,
    phone,
    last_update,
    cdc_capture_at,
    is_deleted
from deduplicate_address
where dup_rank = 1
