{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='rental_id',
        insert_condition='(src.is_deleted = false)',
        update_condition='(src.cdc_capture_at > target.cdc_capture_at or (src.cdc_capture_at = target.cdc_capture_at and src.is_deleted = true) or (target.customer_key = \'-1\' and src.customer_key != \'-1\'))',
        merge_update_columns_rules={
            'rental_date': 'case when src.is_deleted then _old_ else _new_ end',
            'rental_date_day': 'case when src.is_deleted then _old_ else _new_ end',
            'return_date': 'case when src.is_deleted then _old_ else _new_ end',
            'customer_key': 'case when src.is_deleted then _old_ else _new_ end',
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

with new_rentals as (
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

{% if is_incremental() %}
orphaned_rentals as (
    select s.*
    from {{ ref('stg_rental') }} s
    inner join (
        select rental_id
        from {{ this }}
        where customer_key = '-1'
          and is_deleted = false
    ) o on s.rental_id = o.rental_id
),

combined_rentals as (
    select * from new_rentals
    union
    select * from orphaned_rentals
),
{% else %}
combined_rentals as (
    select * from new_rentals
),
{% endif %}

inventory as (
    select * from {{ ref('stg_inventory') }}
),

customers as (
    select
        customer_key,
        customer_id,
        case
            when lag(valid_to) over (partition by customer_id order by valid_from) is null
            then timestamp '1970-01-01 00:00:00'
            else cast(valid_from as timestamp)
        end as effective_from,
        coalesce(cast(valid_to as timestamp), timestamp '9999-12-31 23:59:59') as effective_to
    from {{ ref('dim_customer') }}
),

final as (
    select
        r.rental_id,
        r.rental_date,
        cast(substr(r.rental_date, 1, 10) as date) as rental_date_day,
        r.return_date,
        coalesce(c.customer_key, '-1') as customer_key,
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
    from combined_rentals r
    left join inventory i
        on r.inventory_id = i.inventory_id
    left join customers c
        on r.customer_id = c.customer_id
       and (
           r.rental_date is not null
           and date_parse(r.rental_date, '%Y-%m-%d %H:%i:%s.%f') >= c.effective_from
           and date_parse(r.rental_date, '%Y-%m-%d %H:%i:%s.%f') < c.effective_to
       )
)

select * from final
