select distinct
    

    case education
        when 'unknown' then 0
        when 'basic' then 1
        when 'high school' then 2
        when 'professional course' then 3
        when 'university degree' then 4
        when 'illiterate' then 5
    end

 as education_id,
    education as education_type
from `final-project-df11-group5`.`dwh_final_project`.`int_bank_marketing`
order by education_id asc