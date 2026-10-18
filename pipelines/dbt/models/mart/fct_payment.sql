{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='payment_id',
        insert_condition='(src.is_deleted = false)',
        update_condition='(src.cdc_capture_at > target.cdc_capture_at or (src.cdc_capture_at = target.cdc_capture_at and src.is_deleted = true))',
        merge_update_columns_rules={
            'customer_key': 'case when src.is_deleted then _old_ else _new_ end',
            'customer_id': 'case when src.is_deleted then _old_ else _new_ end',
            'staff_id': 'case when src.is_deleted then _old_ else _new_ end',
            'rental_id': 'case when src.is_deleted then _old_ else _new_ end',
            'film_id': 'case when src.is_deleted then _old_ else _new_ end',
            'store_id': 'case when src.is_deleted then _old_ else _new_ end',
            'amount': 'case when src.is_deleted then _old_ else _new_ end',
            'payment_date': 'case when src.is_deleted then _old_ else _new_ end',
            'payment_date_day': 'case when src.is_deleted then _old_ else _new_ end',
            'is_deleted': 'replace',
            'cdc_capture_at': 'replace'
        },
        on_schema_change='sync_all_columns',
        partitioned_by=['payment_date_day']
    )
}}

with payments as (
    select * from {{ ref('stg_payment') }}
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

rentals as (
    select * from {{ ref('stg_rental') }}
),

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
        p.payment_id,
        coalesce(c.customer_key, '-1') as customer_key,
        p.customer_id,
        p.staff_id,
        p.rental_id,
        i.film_id,
        i.store_id,
        p.amount,
        p.payment_date,
        cast(substr(p.payment_date, 1, 10) as date) as payment_date_day,
        p.cdc_capture_at,
        p.is_deleted
    from payments p
    left join rentals r
        on p.rental_id = r.rental_id
    left join inventory i
        on r.inventory_id = i.inventory_id
    left join customers c
        on p.customer_id = c.customer_id
       and (
           p.payment_date is not null
           and date_parse(p.payment_date, '%Y-%m-%d %H:%i:%s.%f') >= c.effective_from
           and date_parse(p.payment_date, '%Y-%m-%d %H:%i:%s.%f') < c.effective_to
       )
)

select * from final
