-- Rentals should not have dates in the future
select
    rental_id,
    rental_date
from {{ ref('stg_rental') }}
where rental_date > {{ current_timestamp_iso() }}
