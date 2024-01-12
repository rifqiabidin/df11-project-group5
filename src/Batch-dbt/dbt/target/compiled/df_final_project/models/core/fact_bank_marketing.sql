with bank_data as (

    select * from `final-project-df11-group5`.`dwh_final_project`.`cast_bank_marketing`

),  

final as (

    select 
        id,
        join_yearmonth,
        

    case age_group
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

 as age_group_id,
        

    case job
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

 as job_id,
        

    case education
        when 'unknown' then 0
        when 'basic' then 1
        when 'high school' then 2
        when 'professional course' then 3
        when 'university degree' then 4
        when 'illiterate' then 5
    end

 as education_id,
        

    case marital
        when 'unknown' then 0
        when 'single' then 1
        when 'married' then 2
        when 'divorced' then 3
    end

 as marital_id,
        has_default,
        has_housing,
        has_loan
    from bank_data
)

select * from final