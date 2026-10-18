with rentals as (
    select * from {{ ref('stg_rental') }}
    where is_deleted = false
),

payments as (
    select * from {{ ref('stg_payment') }}
    where is_deleted = false
),

customer_rental_summary as (
    select
        r.customer_id,
        min(r.rental_date) as first_rental_date,
        max(r.return_date) as last_rental_date,
        count(distinct r.rental_id) as total_rentals,
        coalesce(sum(p.amount), 0.00) as total_amount_payments,
        coalesce(avg(p.amount), 0.00) as avg_amount_payments
    from rentals r
    left join payments p
        on r.rental_id = p.rental_id
    group by r.customer_id
)

select * from customer_rental_summary
