
    
    

select
    order_id as unique_field,
    count(*) as n_records

from "appdb"."public_intermediate"."int_order_details"
where order_id is not null
group by order_id
having count(*) > 1


