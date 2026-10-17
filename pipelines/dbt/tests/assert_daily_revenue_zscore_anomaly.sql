{{ config(severity="warn", tags=["anomaly_detection"]) }}

with
    revenue_with_stats as (
        select
            payment_date_day,
            store_id,
            total_revenue,
            total_payment,
            avg(total_revenue) over (
                partition by store_id
                order by payment_date_day
                rows between 7 preceding and 1 preceding
            ) as moving_avg_7d,
            stddev(total_revenue) over (
                partition by store_id
                order by payment_date_day
                rows between 7 preceding and 1 preceding
            ) as moving_stddev_7d
        from {{ ref("fct_daily_revenue") }}
    ),

    anomalies as (
        select
            payment_date_day,
            store_id,
            total_revenue,
            moving_avg_7d,
            moving_stddev_7d,
            case
                when moving_stddev_7d is not null and moving_stddev_7d > 0
                then (total_revenue - moving_avg_7d) / moving_stddev_7d
                else 0.0
            end as z_score
        from revenue_with_stats
        where payment_date_day >= date_add('day', -7, current_date)
    )

select
    cast(payment_date_day as varchar) as payment_date_day,
    store_id,
    total_revenue,
    moving_avg_7d,
    moving_stddev_7d,
    z_score
from anomalies
where
    abs(z_score) > cast({{ var("revenue_zscore_threshold", 3.0) }} as double)
    and abs(total_revenue - moving_avg_7d)
    > cast({{ var("revenue_min_deviation_amount", 50.0) }} as double)
