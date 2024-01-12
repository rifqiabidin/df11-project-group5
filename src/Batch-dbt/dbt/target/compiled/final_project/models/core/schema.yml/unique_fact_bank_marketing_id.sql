
    
    

with dbt_test__target as (

  select id as unique_field
  from `df11-group5`.`dwh_final_project`.`fact_bank_marketing`
  where id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


