{{
    config(
        materialized='table',
        unique_key='store_key'
    )
}}

with stores as (
    select * from {{ ref('stg_store') }}
),

addresses as (
    select * from {{ ref('int_full_address') }}
),

managers as (
    select * from {{ ref('stg_staff') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['s.store_id']) }} as store_key,
        s.store_id,
        m.first_name as manager_first_name,
        m.last_name as manager_last_name,
        addr.address,
        addr.address2,
        addr.district,
        addr.city_name,
        addr.country_name,
        addr.postal_code,
        addr.phone
    from stores s
    left join addresses addr
        on s.address_id = addr.address_id
    left join managers m
        on s.manager_staff_id = m.staff_id
)

select * from final
