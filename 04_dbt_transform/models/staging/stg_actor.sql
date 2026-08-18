{{
    config(
        materialized='table',
        unique_key='actor_id'
    )
}}

with raw_actor as (
    select * from {{ source('raw', 'actor') }}
),

selected_columns_actor as (
    select
        actor_id,
        first_name,
        last_name,
        {{ debezium_ts_micros('last_update') }} as last_update,
        {{ debezium_ts_milies('__ts_ms') }} as cdc_capture_at,
        __op,
        cast(__deleted as boolean) as __deleted
    from raw_actor
),

filtered_actor as (
    select *
    from selected_columns_actor
    where __deleted = false or __deleted is null
),

deduplicate_actor as (
    select *,
        row_number() over (partition by actor_id order by cdc_capture_at desc) as dup_rank
    from filtered_actor
)

select
    actor_id,
    first_name,
    last_name,
    last_update
from deduplicate_actor
where dup_rank = 1