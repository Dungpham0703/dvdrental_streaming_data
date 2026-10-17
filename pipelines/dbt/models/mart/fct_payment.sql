{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='payment_id',
        on_schema_change='sync_all_columns',
        partitioned_by=['payment_date_day']
    )
}}

with payments as (
    select * from {{ ref('stg_payment') }}
    {% if is_incremental() %}
    where cdc_capture_at > (select max(cdc_capture_at) from {{ this }})
    {% endif %}
),

rentals as (
    select * from {{ ref('stg_rental') }}
),

inventory as (
    select * from {{ ref('stg_inventory') }}
),

final as (
    select
        p.payment_id,
        p.customer_id,
        p.staff_id,
        p.rental_id,
        i.film_id,
        i.store_id,
        p.amount,
        p.payment_date,
        cast(substr(p.payment_date, 1, 10) as date) as payment_date_day,
        p.cdc_capture_at
    from payments p
    left join rentals r
        on p.rental_id = r.rental_id
    left join inventory i
        on r.inventory_id = i.inventory_id
)

select * from final
