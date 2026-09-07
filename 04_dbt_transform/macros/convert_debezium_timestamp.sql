{% macro debezium_ts_micros(column_name) %}
    date_format(from_unixtime(cast({{ column_name }} as double) / 1000000), '%Y-%m-%d %H:%i:%s.%f')
{% endmacro %}

{% macro debezium_ts_milies(column_name)  %}
    date_format(from_unixtime(cast({{ column_name }} as double) / 1000), '%Y-%m-%d %H:%i:%s.%f')
{% endmacro %}

{% macro debezium_date_days(column_name) %}
    date_format(date_add('day', cast({{ column_name }} as integer), date '1970-01-01'), '%Y-%m-%d')
{% endmacro %}

{% macro current_timestamp_iso() %}
    date_format(current_timestamp, '%Y-%m-%d %H:%i:%s.%f')
{% endmacro %}

{% macro parse_iso_timestamp(column_name) %}
    date_parse({{ column_name }}, '%Y-%m-%d %H:%i:%s.%f')
{% endmacro %}

{% macro snapshot_get_time() %}
    date_format(current_timestamp, '%Y-%m-%d %H:%i:%s.%f')
{% endmacro %}
