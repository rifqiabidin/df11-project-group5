select distinct
    {{ encode_age_group('age_group') }} as age_group_id,
    age_group as age_group
from {{ ref('int_bank_marketing') }}
order by age_group_id asc