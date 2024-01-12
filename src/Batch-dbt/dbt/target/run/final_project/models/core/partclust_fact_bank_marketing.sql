
  
    

    create or replace table `final-project-df11-group5`.`dwh_final_project`.`partclust_fact_bank_marketing`
      
    partition by join_yearmonth
    cluster by age_group_id

    OPTIONS()
    as (
      

SELECT * FROM `final-project-df11-group5`.`dwh_final_project`.`fact_bank_marketing`
    );
  