with updated_stg_bank_marketing as (

    select * from {{ ref('stg_bank_marketing') }}
    {% if is_incremental() %}
    where `timestamp` > (select max(`timestamp`) from {{ this }})
    {% endif %}

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
        {{ grouping_age('age') }} AS age_group,
        {{ remove_dot('job') }} AS job,
        {{ clean_edu('education') }} END AS cleaned_education,
        {{ encode_feature('has_default') }} END AS has_default,
        {{ encode_feature('has_housing') }} END AS has_housing,
        {{ encode_feature('has_loan') }} END AS has_loan
    from transform
)

select * except(cleaned_education, age_group), 
concat(cast(age_group as string), 's') as age_group,
{{ replace_dot('cleaned_education') }} AS education,
from base

