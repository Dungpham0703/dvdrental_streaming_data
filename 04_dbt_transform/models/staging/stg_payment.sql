{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='payment_id',
        on_schema_change='sync_all_columns'
    )
}}

with raw_payment as (
    select * from {{ source('raw', 'payment') }}
    {% if is_incremental() %}
    where {{ debezium_ts_milies('__ts_ms') }} > (select max(cdc_capture_at) from {{ this }})
    {% endif %}
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
    where (__deleted = false or __deleted is null)
      and amount > 0
      and rental_id in (select rental_id from {{ ref('stg_rental') }})
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
    payment_date,
    cdc_capture_at
from deduplicate_payment
where dup_rank = 1
