SELECT COUNT(order_id)
FROM
    "appdb"."public_staging"."stg_orders"
GROUP BY order_id 
HAVING COUNT(order_id) > 1