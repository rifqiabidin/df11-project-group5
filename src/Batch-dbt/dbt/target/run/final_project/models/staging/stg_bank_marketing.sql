

  create or replace view `final-project-df11-group5`.`dwh_final_project`.`stg_bank_marketing`
  OPTIONS()
  as with source as (

    select * from `final-project-df11-group5`.`raw_final_project`.`raw_bank_marketing`

),

base as (

    select
        `Timestamp` as `timestamp`,
        Birth_of_Date as birth_of_date,
        Job as job,
        Marital as marital,
        Education as education,
        `Default` as has_default,
        Housing as has_housing,
        Loan as has_loan,
    from source

)

select * from base;

