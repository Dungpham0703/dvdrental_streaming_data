{{
    config(
        materialized='table',
        unique_key='payment_id'
    )
}}

with raw_payment as (
    select * from {{ source('raw', 'payment') }}
),

selected_columns_payment as (
    select
        payment_id,
        customer_id,
        staff_id,
        rental_id,
        cast(amount as decimal(7,2)) as amount,
        {{ debezium_ts_micros('payment_date') }} as payment_date,
        {{ debezium_ts_milies('__ts_ms') }} as cdc_capture_at,
        __op,
        cast(__deleted as boolean) as __deleted
    from raw_payment
),

filtered_payment as (
    select *
    from selected_columns_payment
    where __deleted = false or __deleted is null
),

deduplicate_payment as (
    select *,
        row_number() over (partition by payment_id order by cdc_capture_at desc) as dup_rank
    from filtered_payment
)

select
    payment_id,
    customer_id,
    staff_id,
    rental_id,
    amount,
    payment_date
from deduplicate_payment
where dup_rank = 1
