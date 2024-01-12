{% macro encode_age_group(column_name) %}

    case {{ column_name }}
        when '10s' then 1
        when '20s' then 2
        when '30s' then 3
        when '40s' then 4
        when '50s' then 5
        when '60s' then 6
        when '70s' then 7
        when '80s' then 8
        when '90s' then 9
    end

{% endmacro %}