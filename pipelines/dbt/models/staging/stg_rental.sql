{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='rental_id',
        insert_condition='(src.is_deleted = false)',
        update_condition='(src.cdc_capture_at > target.cdc_capture_at or (src.cdc_capture_at = target.cdc_capture_at and src.is_deleted = true))',
        merge_update_columns_rules={
            'customer_id': 'case when src.is_deleted then _old_ else _new_ end',
            'inventory_id': 'case when src.is_deleted then _old_ else _new_ end',
            'staff_id': 'case when src.is_deleted then _old_ else _new_ end',
            'rental_date': 'case when src.is_deleted then _old_ else _new_ end',
            'return_date': 'case when src.is_deleted then _old_ else _new_ end',
            'last_update': 'case when src.is_deleted then _old_ else _new_ end',
            'is_deleted': 'replace',
            'cdc_capture_at': 'replace'
        },
        on_schema_change='sync_all_columns'
    )
}}

with raw_rental as (
    select * from {{ source('raw', 'rental') }}
    {% if is_incremental() %}
    where {{ debezium_ts_milies('__ts_ms') }} >= (
        select date_format(
            date_add('hour', -{{ var('cdc_lookback_hours', 1) }}, date_parse(max(cdc_capture_at), '%Y-%m-%d %H:%i:%s.%f')),
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
        case
            when __op = 'd' then true else false
        end as is_deleted
    from raw_rental
),

filtered_rental as (
    select *
    from selected_columns_rental
    where is_deleted = true
       or (
           is_deleted = false
           and (rental_date is null or rental_date <= {{ current_timestamp_iso() }})
           and (return_date is null or return_date >= rental_date)
       )
),

deduplicate_rental as (
    select *,
        row_number() over (
            partition by rental_id
            order by cdc_capture_at desc, is_deleted desc
        ) as dup_rank
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
    cdc_capture_at,
    is_deleted
from deduplicate_rental
where dup_rank = 1
