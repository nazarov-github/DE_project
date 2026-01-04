
  create view "appdb"."public_staging"."stg_categories__dbt_tmp"
    
    
  as (
    SELECT category_id, 
       category_name, 
       parent_category, 
       created_at
FROM "appdb"."public"."categories"
  );