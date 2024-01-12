{{
    config(
        materialized='view'
    )
}}

with int_data as (

    select * from {{ ref('int_bank_marketing') }}

), 
base as (
    select * except(has_default, has_housing, has_loan, str_join_yearmonth),
    cast(has_default as INT) as has_default,
    cast(has_housing as INT) as has_housing,
    cast(has_loan as INT) as has_loan,
    cast(str_join_yearmonth as DATE FORMAT 'YYYY-MM') as join_yearmonth
    from int_data
)
select * from base
