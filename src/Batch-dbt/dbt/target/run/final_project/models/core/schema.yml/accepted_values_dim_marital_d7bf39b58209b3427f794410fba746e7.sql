select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        marital_type as value_field,
        count(*) as n_records

    from `df11-group5`.`dwh_final_project`.`dim_marital`
    group by marital_type

)

select *
from all_values
where value_field not in (
    'married','single','divorced','unknown'
)



      
    ) dbt_internal_test