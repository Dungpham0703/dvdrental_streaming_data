{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='rental_id',
        insert_condition='(src.is_deleted = false)',
        update_condition='(src.cdc_capture_at > target.cdc_capture_at or (src.cdc_capture_at = target.cdc_capture_at and src.is_deleted = true))',
        merge_update_columns_rules={
            'rental_date': 'case when src.is_deleted then _old_ else _new_ end',
            'rental_date_day': 'case when src.is_deleted then _old_ else _new_ end',
            'return_date': 'case when src.is_deleted then _old_ else _new_ end',
            'customer_id': 'case when src.is_deleted then _old_ else _new_ end',
            'film_id': 'case when src.is_deleted then _old_ else _new_ end',
            'store_id': 'case when src.is_deleted then _old_ else _new_ end',
            'staff_id': 'case when src.is_deleted then _old_ else _new_ end',
            'rental_duration_days': 'case when src.is_deleted then _old_ else _new_ end',
            'is_deleted': 'replace',
            'cdc_capture_at': 'replace'
        },
        on_schema_change='sync_all_columns',
        partitioned_by=['rental_date_day']
    )
}}

with rentals as (
    select * from {{ ref('stg_rental') }}
    {% if is_incremental() %}
    where cdc_capture_at >= (
        select date_format(
            date_add('hour', -{{ var('cdc_lookback_hours', 1) }}, date_parse(max(cdc_capture_at), '%Y-%m-%d %H:%i:%s.%f')),
            '%Y-%m-%d %H:%i:%s.%f'
        )
        from {{ this }}
    )
    {% endif %}
),

inventory as (
    select * from {{ ref('stg_inventory') }}
),

final as (
    select
        r.rental_id,
        r.rental_date,
        cast(substr(r.rental_date, 1, 10) as date) as rental_date_day,
        r.return_date,
        r.customer_id,
        i.film_id,
        i.store_id,
        r.staff_id,
        case
            when r.return_date is not null
            then date_diff('day', cast({{ parse_iso_timestamp('r.rental_date') }} as date), cast({{ parse_iso_timestamp('r.return_date') }} as date))
            else null
        end as rental_duration_days,
        r.cdc_capture_at,
        r.is_deleted
    from rentals r
    left join inventory i
        on r.inventory_id = i.inventory_id
)

select * from final
