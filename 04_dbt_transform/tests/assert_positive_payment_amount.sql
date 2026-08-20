-- All payments must have amount > 0
select
    payment_id,
    amount
from {{ ref('stg_payment') }}
where amount <= 0
