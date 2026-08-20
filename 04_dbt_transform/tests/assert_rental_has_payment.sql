{{ config(severity = 'warn') }}

-- Every completed rental (with return_date) should have at least one payment.
select
    r.rental_id,
    r.customer_id
from {{ ref('stg_rental') }} r
left join {{ ref('stg_payment') }} p
    on r.rental_id = p.rental_id
where r.return_date is not null
  and p.payment_id is null
