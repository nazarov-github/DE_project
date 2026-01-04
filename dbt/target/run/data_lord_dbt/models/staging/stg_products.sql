
  create view "appdb"."public_staging"."stg_products__dbt_tmp"
    
    
  as (
    SELECT product_id, 
       product_name, 
       category_id, 
       brand, 
       price, 
       "cost", 
       stock_quantity, 
       weight_kg, 
       dimensions, 
       description, 
       is_active, 
       created_at
FROM "appdb"."public"."pproducts"
  );