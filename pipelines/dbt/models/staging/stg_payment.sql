{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='payment_id',
        insert_condition='(src.is_deleted = false)',
        update_condition='(src.cdc_capture_at > target.cdc_capture_at or (src.cdc_capture_at = target.cdc_capture_at and src.is_deleted = true))',
        merge_update_columns_rules={
            'customer_id': 'case when src.is_deleted then _old_ else _new_ end',
            'staff_id': 'case when src.is_deleted then _old_ else _new_ end',
            'rental_id': 'case when src.is_deleted then _old_ else _new_ end',
            'amount': 'case when src.is_deleted then _old_ else _new_ end',
            'payment_date': 'case when src.is_deleted then _old_ else _new_ end',
            'is_deleted': 'replace',
            'cdc_capture_at': 'replace'
        },
        on_schema_change='sync_all_columns'
    )
}}

with raw_payment as (
    select * from {{ source('raw', 'payment') }}
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

selected_columns_payment as (
    select
        payment_id,
        customer_id,
        staff_id,
        rental_id,
        cast(amount as decimal(7,2)) as amount,
        {{ debezium_ts_micros('payment_date') }} as payment_date,
        {{ debezium_ts_milies('__ts_ms') }} as cdc_capture_at,
        __op,
        case
            when __op = 'd' or cast(__deleted as boolean) = true then true
            else false
        end as is_deleted
    from raw_payment
),

filtered_payment as (
    select *
    from selected_columns_payment
    where is_deleted = true
       or (
           is_deleted = false
           and amount > 0
       )
),

deduplicate_payment as (
    select *,
        row_number() over (
            partition by payment_id
            order by cdc_capture_at desc, is_deleted desc
        ) as dup_rank
    from filtered_payment
)

select
    payment_id,
    customer_id,
    staff_id,
    rental_id,
    amount,
    payment_date,
    cdc_capture_at,
    is_deleted
from deduplicate_payment
where dup_rank = 1
