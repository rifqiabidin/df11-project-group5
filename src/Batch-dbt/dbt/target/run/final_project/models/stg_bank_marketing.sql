

  create or replace view `summer-avenue-401621`.`final_project`.`stg_bank_marketing`
  OPTIONS()
  as 


with source as (

    select * from `summer-avenue-401621`.`final_project`.`raw_bank_marketing`

),

base as (

    select
        cast(age as integer) as age,
        cast(job as string) as job,
        cast(marital as string) as marital_status,
        cast(education as string) as education,
        cast(`default` as string) as in_default,
        cast(housing as string) as housing,
        cast(loan as string) as loan,
        -- CASE 
        --     WHEN default = 'unknown' THEN NULL
        --     ELSE default
        -- END AS default,
        -- CASE 
        --     WHEN housing = 'unknown' THEN NULL
        --     ELSE housing
        -- END AS housing,
        -- CASE 
        --     WHEN loan = 'unknown' THEN NULL
        --     ELSE loan
        -- END AS loan
    from source

)

select * from base;

