select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select job_type
from `df11-group5`.`dwh_final_project`.`dim_job`
where job_type is null



      
    ) dbt_internal_test