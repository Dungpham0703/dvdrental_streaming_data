{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='rental_id',
        on_schema_change='sync_all_columns',
        partitioned_by=['truncate(10, rental_date)']
    )
}}

with rentals as (
    select * from {{ ref('stg_rental') }}
    {% if is_incremental() %}
    where cdc_capture_at > (select max(cdc_capture_at) from {{ this }})
    {% endif %}
),

inventory as (
    select * from {{ ref('stg_inventory') }}
),

final as (
    select
        r.rental_id,
        r.rental_date,
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
        r.cdc_capture_at
    from rentals r
    left join inventory i
        on r.inventory_id = i.inventory_id
)

select * from final
