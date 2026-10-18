{{
    config(
        materialized='view',
        tags=['gold', 'active']
    )
}}

select *
from {{ ref('fct_payment') }}
where is_deleted = false
