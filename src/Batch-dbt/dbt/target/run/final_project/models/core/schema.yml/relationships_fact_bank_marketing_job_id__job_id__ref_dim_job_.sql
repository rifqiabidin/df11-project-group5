select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select job_id as from_field
    from `df11-group5`.`dwh_final_project`.`fact_bank_marketing`
    where job_id is not null
),

parent as (
    select job_id as to_field
    from `df11-group5`.`dwh_final_project`.`dim_job`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test