

SELECT category_id, 
       category_name, 
       parent_category, 
       created_at,
       updated_at
FROM "appdb"."public"."categories_merge"

WHERE updated_at > (SELECT COALESCE(MAX(updated_at), '1900-01-01') FROM "appdb"."public_staging"."stg_categories_merge")
