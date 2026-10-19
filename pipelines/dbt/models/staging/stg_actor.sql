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
        case
            when __op = 'd' or cast(__deleted as boolean) = true then true
            else false
        end as is_deleted
    from raw_actor
),

deduplicate_actor as (
    select *,
        row_number() over (
            partition by actor_id
            order by cdc_capture_at desc, is_deleted desc
        ) as dup_rank
    from selected_columns_actor
)

select
    actor_id,
    first_name,
    last_name,
    last_update,
    cdc_capture_at,
    is_deleted
from deduplicate_actor
where dup_rank = 1
