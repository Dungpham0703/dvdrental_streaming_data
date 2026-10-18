{{ config(severity="error", tags=["data_quality", "reconciliation"]) }}

-- Invariant: fct_daily_revenue total_revenue must strictly equal the sum of active payments
with active_payment_revenue as (
    select
        coalesce(sum(amount), 0.00) as expected_revenue
    from {{ ref("fct_payment_active") }}
),

daily_summary_revenue as (
    select
        coalesce(sum(total_revenue), 0.00) as actual_revenue
    from {{ ref("fct_daily_revenue") }}
)

select
    a.expected_revenue,
    d.actual_revenue,
    abs(a.expected_revenue - d.actual_revenue) as diff
from active_payment_revenue a
cross join daily_summary_revenue d
where abs(a.expected_revenue - d.actual_revenue) > 0.01
