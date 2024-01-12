select distinct
    {{ encode_edu('education') }} as education_id,
    education as education_type
from {{ ref('int_bank_marketing') }}
order by education_id asc