{% macro encode_marital(column_name) %}

    case {{ column_name }}
        when 'unknown' then 0
        when 'single' then 1
        when 'married' then 2
        when 'divorced' then 3
    end

{% endmacro %}