{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='payment_key',
        on_schema_change='sync_all_columns'
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
        {{ dbt_utils.generate_surrogate_key(['p.payment_id']) }} as payment_key,
        p.payment_id,
        p.customer_id,
        p.staff_id,
        p.rental_id,
        i.film_id,
        i.store_id,
        p.amount,
        p.payment_date,
        p.cdc_capture_at
    from payments p
    left join rentals r
        on p.rental_id = r.rental_id
    left join inventory i
        on r.inventory_id = i.inventory_id
)

select * from final