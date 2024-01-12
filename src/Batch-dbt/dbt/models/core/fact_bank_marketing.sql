with bank_data as (

    select * from {{ ref('cast_bank_marketing') }}

),  

final as (

    select 
        id,
        join_yearmonth,
        {{ encode_age_group('age_group') }} as age_group_id,
        {{ encode_job('job') }} as job_id,
        {{ encode_edu('education') }} as education_id,
        {{ encode_marital('marital') }} as marital_id,
        has_default,
        has_housing,
        has_loan
    from bank_data
)

select * from final
