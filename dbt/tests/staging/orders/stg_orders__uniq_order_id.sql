SELECT COUNT(order_id)
FROM
    {{ ref('stg_orders')}}
GROUP BY order_id 
HAVING COUNT(order_id) > 1