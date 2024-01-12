select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select age_group
from `df11-group5`.`dwh_final_project`.`fact_bank_marketing`
where age_group is null



      
    ) dbt_internal_test