
    
    

with all_values as (

    select
        quantity as value_field,
        count(*) as n_records

    from "appdb"."public_intermediate"."int_order_details"
    group by quantity

)

select *
from all_values
where value_field not in (
    > 0
)


