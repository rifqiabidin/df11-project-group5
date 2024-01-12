
  
    

    create or replace table `final-project-df11-group5`.`dwh_final_project`.`dim_marital`
      
    
    

    OPTIONS()
    as (
      select distinct
    

    case marital
        when 'unknown' then 0
        when 'single' then 1
        when 'married' then 2
        when 'divorced' then 3
    end

 as marital_id,
    marital as marital_type
from `final-project-df11-group5`.`dwh_final_project`.`int_bank_marketing`
order by marital_id asc
    );
  