{% macro encode_job(column_name) %}

    case {{ column_name }}
        when 'unknown' then 0
        when 'housemaid' then 1
        when 'services' then 2
        when 'admin' then 3
        when 'blue-collar' then 4
        when 'technician' then 5
        when 'retired' then 6
        when 'management' then 7
        when 'unemployed' then 8
        when 'self-employed' then 9
        when 'entrepreneur' then 10
        when 'student' then 11 
    end

{% endmacro %}