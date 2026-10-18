{{ config(severity="warn", tags=["reconciliation", "watermark"]) }}

-- Detects late-arriving rental events in staging that were bypassed by the fact watermark
with fact_watermark as (
    select max(cdc_capture_at) as max_fact_watermark
    from {{ ref('fct_rental') }}
)

select
    s.rental_id,
    s.cdc_capture_at as staging_cdc_capture_at,
    w.max_fact_watermark
from {{ ref('stg_rental') }} s
cross join fact_watermark w
left join {{ ref('fct_rental') }} f
    on s.rental_id = f.rental_id
where w.max_fact_watermark is not null
  and s.cdc_capture_at <= w.max_fact_watermark
  and f.rental_id is null
