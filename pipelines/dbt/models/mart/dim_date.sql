{{
    config(
        materialized='table',
        table_type='iceberg',
        unique_key='date_key'
    )
}}

with date_sequence as (
    select date_column
    from unnest(sequence(date '2005-01-01', date '2030-12-31', interval '1' day)) as t(date_column)
),

final as (
    select
        cast(date_format(date_column, '%Y%m%d') as integer) as date_key,
        cast(date_column as date) as date_day,
        year(date_column) as year,
        quarter(date_column) as quarter,
        month(date_column) as month,
        date_format(date_column, '%M') as month_name,
        day(date_column) as day_of_month,
        day_of_week(date_column) as day_of_week,
        date_format(date_column, '%W') as day_name,
        case when day_of_week(date_column) in (6, 7) then true else false end as is_weekend
    from date_sequence
)

select * from final
