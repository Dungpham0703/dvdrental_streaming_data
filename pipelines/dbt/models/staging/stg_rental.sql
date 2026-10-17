{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='rental_id',
        on_schema_change='sync_all_columns'
    )
}}

with raw_rental as (
    select * from {{ source('raw', 'rental') }}
    {% if is_incremental() %}
    where {{ debezium_ts_milies('__ts_ms') }} >= (
        select date_format(
            date_add('hour', -1, date_parse(max(cdc_capture_at), '%Y-%m-%d %H:%i:%s.%f')),
            '%Y-%m-%d %H:%i:%s.%f'
        )
        from {{ this }}
    )
    {% endif %}
),

selected_columns_rental as (
    select
        rental_id,
        customer_id,
        inventory_id,
        staff_id,
        {{ debezium_ts_micros('rental_date') }} as rental_date,
        {{ debezium_ts_micros('return_date') }} as return_date,
        {{ debezium_ts_micros('last_update') }} as last_update,
        {{ debezium_ts_milies('__ts_ms') }} as cdc_capture_at,
        __op,
        cast(__deleted as boolean) as __deleted
    from raw_rental
),

filtered_rental as (
    select *
    from selected_columns_rental
    where (__deleted = false or __deleted is null)
      and (rental_date is null or rental_date <= {{ current_timestamp_iso() }})
      and (return_date is null or return_date >= rental_date)
),

deduplicate_rental as (
    select *,
        row_number() over (partition by rental_id order by cdc_capture_at desc) as dup_rank
    from filtered_rental
)

select
    rental_id,
    customer_id,
    inventory_id,
    staff_id,
    rental_date,
    return_date,
    last_update,
    cdc_capture_at
from deduplicate_rental
where dup_rank = 1
