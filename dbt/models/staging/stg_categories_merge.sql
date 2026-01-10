{{
    config(
        materialized='incremental',
        unique_key='category_id',
        schema='staging',
        incremental_strategy='merge',
        merge_update_columns = ['category_name', 'parent_category','updated_at']

    )
}}

SELECT category_id, 
       category_name, 
       parent_category, 
       created_at,
       updated_at
FROM {{ source("raw", "categories_merge")}}
{% if is_incremental() %}
WHERE updated_at > (SELECT COALESCE(MAX(updated_at), '1900-01-01') FROM {{ this }})
{% endif %}