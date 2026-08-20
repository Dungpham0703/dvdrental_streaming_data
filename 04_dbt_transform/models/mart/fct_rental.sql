{{
    config(
        materialized='table',
        unique_key='rental_key'
    )
}}

with rentals as (
    select * from {{ ref('stg_rental') }}
),

inventory as (
    select * from {{ ref('stg_inventory') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['r.rental_id']) }} as rental_key,
        r.rental_id,
        r.rental_date,
        r.return_date,
        r.customer_id,
        i.film_id,
        i.store_id,
        r.staff_id,
        case
            when r.return_date is not null
            then date_diff('day', cast(r.rental_date as date), cast(r.return_date as date))
            else null
        end as rental_duration_days
    from rentals r
    left join inventory i
        on r.inventory_id = i.inventory_id
)

select * from final
