with addresses as (
    select * from {{ ref('stg_address') }}
),

cities as (
    select * from {{ ref('stg_city') }}
),

countries as (
    select * from {{ ref('stg_country') }}
)

select
    a.address_id,
    a.address,
    a.address2,
    a.district,
    a.postal_code,
    a.phone,
    ci.city_name,
    co.country_name
from addresses a
left join cities ci
    on a.city_id = ci.city_id
left join countries co
    on ci.country_id = co.country_id
