{{
    config(
        materialized='table',
        unique_key='customer_id'
    )
}}

with raw_customer as (
    select * from {{ source('raw', 'customer') }}
),

selected_columns_customer as (
    select
        customer_id,
        store_id,
        first_name,
        last_name,
        email,
        address_id,
        cast(activebool as boolean) as is_active,
        active,
        {{ debezium_date_days('create_date') }} as create_date,
        {{ debezium_ts_micros('last_update') }} as last_update,
        {{ debezium_ts_milies('__ts_ms') }} as cdc_capture_at,
        case
            when __op = 'd' or cast(__deleted as boolean) = true then true
            else false
        end as is_deleted
    from raw_customer
),

deduplicate_customer as (
    select *,
        row_number() over (
            partition by customer_id
            order by cdc_capture_at desc, is_deleted desc
        ) as dup_rank
    from selected_columns_customer
)

select
    customer_id,
    store_id,
    first_name,
    last_name,
    email,
    address_id,
    is_active,
    active,
    create_date,
    last_update,
    cdc_capture_at,
    is_deleted
from deduplicate_customer
where dup_rank = 1
