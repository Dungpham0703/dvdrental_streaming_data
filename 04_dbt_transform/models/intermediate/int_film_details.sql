with films as (
    select * from {{ ref('stg_film') }}
),

languages as (
    select * from {{ ref('stg_language') }}
),

film_categories as (
    select * from {{ ref('stg_film_category') }}
),

categories as (
    select * from {{ ref('stg_category') }}
)

select
    f.film_id,
    f.title,
    f.description,
    f.release_year,
    l.language_name,
    c.category_name,
    f.rental_duration,
    f.rental_rate,
    f.film_length_minutes,
    f.replacement_cost,
    f.rating,
    f.special_features,
    f.last_update
from films f
left join languages l
    on f.language_id = l.language_id
left join film_categories fc
    on f.film_id = fc.film_id
left join categories c
    on fc.category_id = c.category_id
