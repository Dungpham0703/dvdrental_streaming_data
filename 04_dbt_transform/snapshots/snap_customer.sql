{% snapshot snap_customer %}

{{
    config(
        target_schema='dvdrental_staging',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='last_update',
        table_type='iceberg'
    )
}}

select * from {{ ref('stg_customer') }}

{% endsnapshot %}
