
    
    

with all_values as (

    select
        item_total as value_field,
        count(*) as n_records

    from "appdb"."public_intermediate"."int_order_details"
    group by item_total

)

select *
from all_values
where value_field not in (
    >= 0
)


