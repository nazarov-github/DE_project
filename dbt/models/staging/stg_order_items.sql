SELECT order_item_id, 
       order_id, 
       product_id, 
       quantity, 
       unit_price, 
       line_total, 
       discount_amount
FROM {{ source("raw", "order_items")}}
