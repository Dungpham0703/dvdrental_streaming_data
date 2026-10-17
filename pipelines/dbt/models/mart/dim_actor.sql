{{
    config(
        materialized='table',
        unique_key='actor_id'
    )
}}

with actors as (
    select * from {{ ref('stg_actor') }}
),

film_counts as (
    select
        actor_id,
        count(*) as film_count
    from {{ ref('stg_film_actor') }}
    group by actor_id
),

final as (
    select
        a.actor_id,
        a.first_name,
        a.last_name,
        coalesce(fc.film_count, 0) as film_count
    from actors a
    left join film_counts fc
        on a.actor_id = fc.actor_id
)

select * from final
