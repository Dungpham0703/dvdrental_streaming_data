with source as (
        select * from {{ source('raw', 'store') }}
  ),
  renamed as (
      select
          {{ adapter.quote("_hoodie_commit_time") }},
        {{ adapter.quote("_hoodie_commit_seqno") }},
        {{ adapter.quote("_hoodie_record_key") }},
        {{ adapter.quote("_hoodie_partition_path") }},
        {{ adapter.quote("_hoodie_file_name") }},
        {{ adapter.quote("store_id") }},
        {{ adapter.quote("manager_staff_id") }},
        {{ adapter.quote("address_id") }},
        {{ adapter.quote("last_update") }},
        {{ adapter.quote("__ts_ms") }},
        {{ adapter.quote("__op") }},
        {{ adapter.quote("__deleted") }}

      from source
  )
  select * from renamed
    