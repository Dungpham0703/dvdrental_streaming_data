{% macro debezium_ts_micros(column_name) %}
    from_unixtime(cast({{ column_name }} as double) / 1000000)
{% endmacro %}

{% macro debezium_ts_milies(column_name)  %}
    from_unixtime(cast({{ column_name }} as double) / 1000)
{% endmacro %}

{% macro debezium_date_days(column_name) %}
    date_add('day', cast({{ column_name }} as integer), date '1970-01-01')
{% endmacro %}