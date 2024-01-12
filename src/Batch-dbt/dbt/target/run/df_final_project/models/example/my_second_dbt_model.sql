

  create or replace view `df11-group5`.`dwh_final_project`.`my_second_dbt_model`
  OPTIONS()
  as -- Use the `ref` function to select from other models

select *
from `df11-group5`.`dwh_final_project`.`my_first_dbt_model`
where id = 1;

