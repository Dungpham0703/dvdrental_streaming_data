{{ config(severity="warn", tags=["completeness"]) }}


with
    hourly_stats as (
        select
            date_trunc(
                'hour', date_parse(cdc_capture_at, '%Y-%m-%d %H:%i:%s.%f')
            ) as capture_hour,
            count(distinct payment_id) as record_count
        from {{ ref("stg_payment") }}
        where
            date_parse(cdc_capture_at, '%Y-%m-%d %H:%i:%s.%f')
            >= date_add('day', -8, current_timestamp)
        group by 1
    ),

    latest_hour_stat as (
        select capture_hour, record_count as current_volume
        from hourly_stats
        where capture_hour = (select max(capture_hour) from hourly_stats)
    ),

    historical_baseline as (
        select
            avg(record_count) as avg_expected_volume,
            stddev(record_count) as stddev_volume
        from hourly_stats h
        cross join latest_hour_stat l
        where
            h.capture_hour < l.capture_hour
            and extract(hour from h.capture_hour) = extract(hour from l.capture_hour)
    )

select
    l.capture_hour,
    l.current_volume,
    b.avg_expected_volume,
    (l.current_volume * 1.0 / nullif(b.avg_expected_volume, 0)) as volume_ratio
from latest_hour_stat l
cross join historical_baseline b
where
    b.avg_expected_volume
    >= cast({{ var("completeness_min_expected_volume", 20) }} as integer)
    and l.current_volume < (
        b.avg_expected_volume
        * cast({{ var("completeness_drop_threshold_ratio", 0.2) }} as double)
    )
