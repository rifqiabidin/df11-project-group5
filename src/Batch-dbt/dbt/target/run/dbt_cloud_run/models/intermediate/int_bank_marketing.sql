-- back compat for old kwarg name
  
  
        
    

    

    merge into `final-project-df11-group5`.`dwh_final_project`.`int_bank_marketing` as DBT_INTERNAL_DEST
        using (with updated_stg_bank_marketing as (

    select * from `final-project-df11-group5`.`dwh_final_project`.`stg_bank_marketing`
    
    where `timestamp` > (select max(`timestamp`) from `final-project-df11-group5`.`dwh_final_project`.`int_bank_marketing`)
    

),

transform as (
    select * except (
        birth_of_date),
        GENERATE_UUID() as id,
        --md5(`timestamp` || birth_of_date) as id,
        DATE_DIFF(PARSE_DATE('%d/%m/%Y',FORMAT_DATE('%d/%m/%Y', CURRENT_DATE('Asia/Jakarta'))), birth_of_date, YEAR)  AS age
    from updated_stg_bank_marketing
),

base as (
    select * except (
        age, 
        job, 
        education,
        has_default,
        has_housing,
        has_loan
        ),
        FORMAT_DATETIME('%Y-%m', `Timestamp`) AS str_join_yearmonth,
        
    FLOOR(age / 10) *10
 AS age_group,
        
    TRIM(job, '.')
 AS job,
        
    CASE
        WHEN education IN ('basic.4y','basic.6y','basic.9y')
        THEN 'basic'
        ELSE education
 END AS cleaned_education,
        
    CASE 
        WHEN has_default = 'yes' then '1'
        WHEN has_default = 'no' then '0'
        ELSE NULL
 END AS has_default,
        
    CASE 
        WHEN has_housing = 'yes' then '1'
        WHEN has_housing = 'no' then '0'
        ELSE NULL
 END AS has_housing,
        
    CASE 
        WHEN has_loan = 'yes' then '1'
        WHEN has_loan = 'no' then '0'
        ELSE NULL
 END AS has_loan
    from transform
)

select * except(cleaned_education, age_group), 
concat(cast(age_group as string), 's') as age_group,

    REPLACE(cleaned_education, '.', ' ')
 AS education,
from base
        ) as DBT_INTERNAL_SOURCE
        on (FALSE)

    

    when not matched then insert
        (`timestamp`, `marital`, `id`, `str_join_yearmonth`, `job`, `has_default`, `has_housing`, `has_loan`, `age_group`, `education`)
    values
        (`timestamp`, `marital`, `id`, `str_join_yearmonth`, `job`, `has_default`, `has_housing`, `has_loan`, `age_group`, `education`)


    