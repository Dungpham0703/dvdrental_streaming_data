{{
    config(
        materialized='table',
        unique_key='film_id'
    )
}}

with raw_film as (
    select * from {{ source('raw', 'film') }}
),

selected_columns_film as (
    select
        film_id,
        title,
        description,
        release_year,
        language_id,
        rental_duration,
        cast(rental_rate as decimal(5,2)) as rental_rate,
        length as film_length_minutes,
        cast(replacement_cost as decimal(5,2)) as replacement_cost,
        rating,
        special_features,
        {{ debezium_ts_micros('last_update') }} as last_update,
        {{ debezium_ts_milies('__ts_ms') }} as cdc_capture_at,
        __op,
        cast(__deleted as boolean) as __deleted
    from raw_film
),

filtered_film as (
    select *
    from selected_columns_film
    where __deleted = false or __deleted is null
),

deduplicate_film as (
    select *,
        row_number() over (partition by film_id order by cdc_capture_at desc) as dup_rank
    from filtered_film
)

select
    film_id,
    title,
    description,
    release_year,
    language_id,
    rental_duration,
    rental_rate,
    film_length_minutes,
    replacement_cost,
    rating,
    special_features,
    last_update
from deduplicate_film
where dup_rank = 1
