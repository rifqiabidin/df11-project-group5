select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        job_type as value_field,
        count(*) as n_records

    from `df11-group5`.`dwh_final_project`.`dim_job`
    group by job_type

)

select *
from all_values
where value_field not in (
    'housemaid','services','admin','blue-collar','technician','retired','management','unemployed','self-employed','unknown','entrepreneur','student'
)



      
    ) dbt_internal_test