select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select join_yearmonth
from `df11-group5`.`dwh_final_project`.`fact_bank_marketing`
where join_yearmonth is null



      
    ) dbt_internal_test