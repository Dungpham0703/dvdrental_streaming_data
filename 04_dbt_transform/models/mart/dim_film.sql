{{
    config(
        materialized='table',
        unique_key='film_key'
    )
}}

with film_details as (
    select * from {{ ref('int_film_details') }}
),

actor_counts as (
    select
        film_id,
        count(*) as actor_count
    from {{ ref('stg_film_actor') }}
    group by film_id
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['fd.film_id']) }} as film_key,
        fd.film_id,
        fd.title,
        fd.description,
        fd.release_year,
        fd.language_name,
        fd.category_name,
        fd.rental_duration,
        fd.rental_rate,
        fd.film_length_minutes,
        fd.replacement_cost,
        fd.rating,
        fd.special_features,
        coalesce(ac.actor_count, 0) as actor_count
    from film_details fd
    left join actor_counts ac
        on fd.film_id = ac.film_id
)

select * from final
