
    
    

with child as (
    select marital_id as from_field
    from `df11-group5`.`dwh_final_project`.`fact_bank_marketing`
    where marital_id is not null
),

parent as (
    select marital_id as to_field
    from `df11-group5`.`dwh_final_project`.`dim_marital`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


