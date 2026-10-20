{% macro debezium_ts_micros(column_name) %}
    cast(from_unixtime(cast({{ column_name }} as double) / 1000000.0) as timestamp(3))
{% endmacro %}

{% macro debezium_ts_milies(column_name)  %}
    cast(from_unixtime(cast({{ column_name }} as double) / 1000.0) as timestamp(3))
{% endmacro %}

{% macro debezium_date_days(column_name) %}
    case
        when {{ column_name }} is null or trim(cast({{ column_name }} as varchar)) = '' then null
        when cast({{ column_name }} as varchar) like '%-%'
            then cast(substr(cast({{ column_name }} as varchar), 1, 10) as date)
        else cast(date_add('day', cast({{ column_name }} as integer), date '1970-01-01') as date)
    end
{% endmacro %}

{% macro current_timestamp_iso() %}
    cast(current_timestamp as timestamp(3))
{% endmacro %}

{% macro parse_iso_timestamp(column_name) %}
    cast({{ column_name }} as timestamp(3))
{% endmacro %}

{% macro snapshot_get_time() %}
    cast(current_timestamp as timestamp(3))
{% endmacro %}
