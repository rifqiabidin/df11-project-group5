select distinct
    

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
    age_group as age_group
from `final-project-df11-group5`.`dwh_final_project`.`int_bank_marketing`
order by age_group_id asc