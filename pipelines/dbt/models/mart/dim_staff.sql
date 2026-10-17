{{
    config(
        materialized='table',
        unique_key='staff_id'
    )
}}

with staff as (
    select * from {{ ref('stg_staff') }}
),

addresses as (
    select * from {{ ref('int_full_address') }}
),

final as (
    select
        st.staff_id,
        st.first_name,
        st.last_name,
        st.email,
        st.username,
        st.store_id,
        st.is_active,
        addr.address,
        addr.address2,
        addr.district,
        addr.city_name,
        addr.country_name,
        addr.postal_code,
        addr.phone
    from staff st
    left join addresses addr
        on st.address_id = addr.address_id
)

select * from final
