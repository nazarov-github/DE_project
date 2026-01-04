
  create view "appdb"."public_staging"."stg_orders__dbt_tmp"
    
    
  as (
    SELECT order_id, 
       customer_id, 
       order_date, 
       status as order_status, 
       payment_method, 
       shipping_address, 
       billing_address, 
       discount_amount, 
       tax_amount, 
       shipping_cost, 
       total_amount, 
       currency, 
       created_at, 
       updated_at, 
       subtotal
FROM "appdb"."public"."orders"
  );