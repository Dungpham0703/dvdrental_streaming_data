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
),

film_category_agg as (
    select
        fc.film_id,
        array_join(array_agg(distinct c.category_name), ', ') as category_name
    from film_categories fc
    left join categories c
        on fc.category_id = c.category_id
    where coalesce(fc.is_deleted, false) = false
      and coalesce(c.is_deleted, false) = false
    group by fc.film_id
)

select
    f.film_id,
    f.title,
    f.description,
    f.release_year,
    l.language_name,
    coalesce(fca.category_name, 'Uncategorized') as category_name,
    f.rental_duration,
    f.rental_rate,
    f.film_length_minutes,
    f.replacement_cost,
    f.rating,
    f.special_features,
    f.last_update,
    f.is_deleted
from films f
left join languages l
    on f.language_id = l.language_id
left join film_category_agg fca
    on f.film_id = fca.film_id
