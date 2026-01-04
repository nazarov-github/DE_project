
  
    

  create  table "appdb"."public_intermediate"."int_order_details__dbt_tmp"
  
  
    as
  
  (
    SELECT o.order_id,
       o.order_date,
       o.customer_id,
       o.order_status,
       p.product_id,
       p.product_name,
       c.category_name,
       oi.unit_price,
       oi.quantity,
       oi.quantity * oi.unit_price as item_total,
       current_timestamp as _loaded_at
FROM "appdb"."public_staging"."stg_orders" as o
JOIN "appdb"."public_staging"."stg_order_items" as oi ON o.order_id = oi.order_item_id 
JOIN "appdb"."public_staging"."stg_products" as p ON oi.product_id = p.product_id 
JOIN "appdb"."public_staging"."stg_categories" as c ON p.category_id = c.category_id
  );
  