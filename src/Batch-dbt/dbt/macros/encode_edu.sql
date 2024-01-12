{% macro encode_edu(column_name) %}

    case {{ column_name }}
        when 'unknown' then 0
        when 'basic' then 1
        when 'high school' then 2
        when 'professional course' then 3
        when 'university degree' then 4
        when 'illiterate' then 5
    end

{% endmacro %}