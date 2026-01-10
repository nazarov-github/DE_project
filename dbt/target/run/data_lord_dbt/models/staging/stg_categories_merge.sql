
      -- back compat for old kwarg name
  
  
        
            
            
        
    

    

    merge into "appdb"."public_staging"."stg_categories_merge" as DBT_INTERNAL_DEST
        using "stg_categories_merge__dbt_tmp010756490342" as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.category_id = DBT_INTERNAL_DEST.category_id
            )

    
    when matched then update set
        category_name = DBT_INTERNAL_SOURCE.category_name,parent_category = DBT_INTERNAL_SOURCE.parent_category,updated_at = DBT_INTERNAL_SOURCE.updated_at
    

    when not matched then insert
        ("category_id", "category_name", "parent_category", "created_at", "updated_at")
    values
        ("category_id", "category_name", "parent_category", "created_at", "updated_at")


  