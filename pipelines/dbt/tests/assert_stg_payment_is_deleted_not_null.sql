{{ config(severity="error", tags=["data_quality", "cdc_deletions"]) }}

-- Invariant: Every record in stg_payment must have an explicit boolean is_deleted flag (not null)
select
    payment_id,
    customer_id,
    cdc_capture_at,
    is_deleted
from {{ ref("stg_payment") }}
where is_deleted is null
