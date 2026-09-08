{{
    config (
        materialized='table',
        table_type='iceberg',
        unique_key='revenue_key',
        partitioned_by=['payment_date_day']
    )
}}

with dates as (
    select * from {{ ref('dim_date') }}
    where date_day >= (select min(payment_date_day) from {{ ref('fct_payment') }})
      and date_day <= (select max(payment_date_day) from {{ ref('fct_payment') }})
),

stores as (
    select * from {{ ref('dim_store') }}
),

date_store_grid as (
    select
        d.date_day as payment_date_day,
        s.store_id
    from dates d
    cross join stores s
),

payments as (
    select * from {{ ref('fct_payment') }}
),

final as (
    select
        cast(concat(date_format(g.payment_date_day, '%Y%m%d'), '_', cast(g.store_id as varchar)) as varchar) as revenue_key,
        g.payment_date_day,
        g.store_id,
        coalesce(count(distinct p.payment_id), 0) as total_payment,
        coalesce(count(distinct p.customer_id), 0) as total_paying_customer,
        coalesce(sum(p.amount), 0.00) as total_revenue,
        coalesce(avg(p.amount), 0.00) as avg_payment_amount
    from date_store_grid g
    left join payments p 
        on g.payment_date_day = p.payment_date_day 
       and g.store_id = p.store_id
    group by g.payment_date_day, g.store_id
)

select * from final