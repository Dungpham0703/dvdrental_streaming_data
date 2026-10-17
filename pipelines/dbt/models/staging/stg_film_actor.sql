{{
    config(
        materialized='table',
        unique_key=['film_id', 'actor_id']
    )
}}

with raw_film_actor as (
    select * from {{ source('raw', 'film_actor') }}
),

selected_columns_film_actor as (
    select
        film_id,
        actor_id,
        {{ debezium_ts_micros('last_update') }} as last_update,
        {{ debezium_ts_milies('__ts_ms') }} as cdc_capture_at,
        __op,
        cast(__deleted as boolean) as __deleted
    from raw_film_actor
),

filtered_film_actor as (
    select *
    from selected_columns_film_actor
    where __deleted = false or __deleted is null
),

deduplicate_film_actor as (
    select *,
        row_number() over (partition by film_id, actor_id order by cdc_capture_at desc) as dup_rank
    from filtered_film_actor
)

select
    film_id,
    actor_id,
    last_update
from deduplicate_film_actor
where dup_rank = 1
