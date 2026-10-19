{% snapshot snap_customer %}

{{
    config(
        target_schema='dvdrental_staging',
        unique_key='customer_id',
        strategy='check',
        check_cols=[
            'store_id',
            'first_name',
            'last_name',
            'email',
            'address_id',
            'is_active',
            'active',
            'is_deleted'
        ],
        table_type='iceberg'
    )
}}

select * from {{ ref('stg_customer') }}

{% endsnapshot %}
