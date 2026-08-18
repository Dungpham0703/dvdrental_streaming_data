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
        __op,
        cast(__deleted as boolean) as __deleted
    from raw_customer
),

filtered_customer as (
    select *
    from selected_columns_customer
    where __deleted = false or __deleted is null
),

deduplicate_customer as (
    select *,
        row_number() over (partition by customer_id order by cdc_capture_at desc) as dup_rank
    from filtered_customer
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
    last_update
from deduplicate_customer
where dup_rank = 1