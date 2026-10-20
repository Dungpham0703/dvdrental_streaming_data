{{
    config(
        severity=var("freshness_test_severity", "warn"),
        tags=["freshness", "pre_check"],
    )
}}

with
    latest_event as (
        select
            max(from_unixtime(cast(__ts_ms as double) / 1000.0)) as last_cdc_timestamp
        from {{ source("raw", "rental") }}
    )

select
    last_cdc_timestamp,
    current_timestamp as checked_at,
    date_diff('minute', last_cdc_timestamp, current_timestamp) as lag_minutes
from latest_event
where
    date_diff('minute', last_cdc_timestamp, current_timestamp)
    > cast({{ var("freshness_max_lag_minutes", 45) }} as integer)
    or last_cdc_timestamp is null
