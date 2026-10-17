{{ config(severity="warn", tags=["completeness", "reconciliation"]) }}

with
    payment_reconciliation as (
        select
            count(*) as total_payments,
            count(
                case when p.rental_id is not null and r.rental_id is null then 1 end
            ) as orphan_payments
        from {{ ref("stg_payment") }} p
        left join {{ ref("stg_rental") }} r on p.rental_id = r.rental_id
    )

select
    total_payments,
    orphan_payments,
    (orphan_payments * 100.0 / nullif(total_payments, 0)) as orphan_percentage
from payment_reconciliation
where
    total_payments > 0
    and (orphan_payments * 100.0 / nullif(total_payments, 0))
    > cast({{ var("max_orphan_payment_percentage", 5.0) }} as double)
