{{
    config(
        materialized='table', 
        table_type='iceberg',
        tags=['gold']
    )
}}

with dim_customer as (
    select * from {{ ref('dim_customer') }}
),

customer_rental_summary as (
    select * from {{ ref('int_customer_rental_summary') }}
),

final as (
    select 
        dc.customer_key,
        dc.customer_id,
        dc.store_id,
        dc.first_name,
        dc.last_name,
        dc.email,
        dc.address,
        dc.address2,
        dc.district,
        dc.city_name,
        dc.country_name,
        dc.postal_code,
        dc.phone,
        dc.is_active,
        dc.active,
        dc.create_date,
        dc.valid_from,
        dc.valid_to,
        dc.is_current,

        crs.first_rental_date,
        crs.last_rental_date,
        coalesce(crs.total_rentals, 0) as total_rentals,
        coalesce(crs.total_amount_payments, 0.00) as total_amount_payments,
        coalesce(crs.avg_amount_payments, 0.00) as avg_amount_payments,

        case 
            when coalesce(crs.total_amount_payments, 0) >= 150 then 'VIP'
            when coalesce(crs.total_amount_payments, 0) >= 100 then 'Gold'
            when coalesce(crs.total_amount_payments, 0) >= 50 then 'Silver'
            when coalesce(crs.total_amount_payments, 0) > 0 then 'Bronze'
            else 'Non-paying'
        end as customer_tier

    from dim_customer dc
    left join customer_rental_summary crs 
        on dc.customer_id = crs.customer_id
    where dc.is_current = true
)

select * from final