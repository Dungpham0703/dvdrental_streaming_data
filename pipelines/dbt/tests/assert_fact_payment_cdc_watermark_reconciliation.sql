{{ config(severity="warn", tags=["reconciliation", "watermark"]) }}

with fact_watermark as (
    select max(cdc_capture_at) as max_fact_watermark
    from {{ ref('fct_payment') }}
)

select
    s.payment_id,
    s.cdc_capture_at as staging_cdc_capture_at,
    w.max_fact_watermark
from {{ ref('stg_payment') }} s
cross join fact_watermark w
left join {{ ref('fct_payment') }} f
    on s.payment_id = f.payment_id
where w.max_fact_watermark is not null
  and s.cdc_capture_at <= w.max_fact_watermark
  and f.payment_id is null
