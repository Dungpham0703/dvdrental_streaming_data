{{
    config(
        materialized='table',
        unique_key='customer_key'
    )
}}

with snapshot_customer as (
    select * from {{ ref('snap_customer') }}
),

addresses as (
    select * from {{ ref('int_full_address') }}
),

final as (
    select
        sc.dbt_scd_id as customer_key,
        sc.customer_id,

        sc.first_name,
        sc.last_name,
        sc.email,
        sc.store_id,
        sc.is_active,
        sc.active,
        sc.create_date,

        addr.address,
        addr.address2,
        addr.district,
        addr.city_name,
        addr.country_name,
        addr.postal_code,
        addr.phone,

        sc.dbt_valid_from as valid_from,
        sc.dbt_valid_to as valid_to,
        case
            when sc.dbt_valid_to is null then true
            else false
        end as is_current,
        coalesce(sc.is_deleted, false) as is_deleted

    from snapshot_customer sc
    left join addresses addr
        on sc.address_id = addr.address_id
)

select * from final
