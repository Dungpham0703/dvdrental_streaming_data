{{ config(severity="error", tags=["data_quality", "cdc_deletions"]) }}

-- Invariant: Active views must never return records with is_deleted = true
with deleted_rentals as (
    select
        rental_id,
        'fct_rental_active' as model_name
    from {{ ref("fct_rental_active") }}
    where is_deleted = true
),

deleted_payments as (
    select
        payment_id as rental_id,
        'fct_payment_active' as model_name
    from {{ ref("fct_payment_active") }}
    where is_deleted = true
)

select * from deleted_rentals
union all
select * from deleted_payments
