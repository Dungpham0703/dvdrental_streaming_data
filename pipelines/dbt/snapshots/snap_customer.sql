{% snapshot snap_customer %}

{{
    config(
        target_schema='dvdrental_staging',
        unique_key='customer_id',
        strategy='check',
        check_cols='all',
        table_type='iceberg'
    )
}}

select * from {{ ref('stg_customer') }}

{% endsnapshot %}
