
  create view "appdb"."public_staging"."stg_reviews__dbt_tmp"
    
    
  as (
    SELECT review_id, 
       customer_id, 
       product_id, 
       rating, 
       title, 
       "comment" as customer_comment, 
       is_verified_purchase, 
       helpful_votes, 
       created_at
FROM "appdb"."public"."reviews"
  );