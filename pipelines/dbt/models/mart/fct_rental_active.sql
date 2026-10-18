{{
    config(
        materialized='view',
        tags=['gold', 'active']
    )
}}

select *
from {{ ref('fct_rental') }}
where is_deleted = false
