{{
    config(
        materialized='table',
        unique_key='payment_key'
    )
}}

with payments as (
    select * from {{ ref('stg_payment') }}
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
        p.payment_date
    from payments p
    left join rentals r
        on p.rental_id = r.rental_id
    left join inventory i
        on r.inventory_id = i.inventory_id
)

select * from final
