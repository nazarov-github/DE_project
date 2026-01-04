SELECT log_id, 
       product_id, 
       movement_type, 
       quantity_change, 
       reason, 
       "timestamp" as log_time, 
       reference_id, 
       notes
FROM {{ source("raw", "inventory_logs")}}
