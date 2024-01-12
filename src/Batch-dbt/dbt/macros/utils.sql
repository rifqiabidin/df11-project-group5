{% macro clean_edu(column_name) %}
    CASE
        WHEN {{column_name}} IN ('basic.4y','basic.6y','basic.9y')
        THEN 'basic'
        ELSE {{column_name}}
{% endmacro %}

{% macro grouping_age(column_name) %}
    FLOOR({{column_name}} / 10) *10
{% endmacro %}

{% macro encode_feature(column_name) %}
    CASE 
        WHEN {{column_name}} = 'yes' then '1'
        WHEN {{column_name}} = 'no' then '0'
        ELSE NULL
{% endmacro %}

{% macro remove_dot(column_name) %}
    TRIM({{ column_name }}, '.')
{% endmacro %}

{% macro replace_dot(column_name) %}
    REPLACE({{ column_name }}, '.', ' ')
{% endmacro %}