
  create view "appdb"."public_staging"."stg_order_items__dbt_tmp"
    
    
  as (
    SELECT order_item_id, 
       order_id, 
       product_id, 
       quantity, 
       unit_price, 
       line_total, 
       discount_amount
FROM "appdb"."public"."order_items"
  );