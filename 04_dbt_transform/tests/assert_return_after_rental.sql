-- Return date must be after or equal to rental date
select
    rental_id,
    rental_date,
    return_date
from {{ ref('stg_rental') }}
where return_date is not null
  and return_date < rental_date
