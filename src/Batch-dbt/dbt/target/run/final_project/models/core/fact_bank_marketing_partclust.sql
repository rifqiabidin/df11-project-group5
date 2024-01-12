
  
    

    create or replace table `df11-group5`.`dwh_final_project`.`fact_bank_marketing_partclust`
      
    
    cluster by age_group

    OPTIONS()
    as (
      

SELECT * FROM `df11-group5`.`dwh_final_project`.`fact_bank_marketing`
    );
  