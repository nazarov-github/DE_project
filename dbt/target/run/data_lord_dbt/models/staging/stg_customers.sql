
  create view "appdb"."public_staging"."stg_customers__dbt_tmp"
    
    
  as (
    

select customer_id,
       email,
       first_name,
       last_name,
       phone,
       date_of_birth,
       gender,
       country,
       city,
       postal_code,
       address,
       registration_date,
       last_login,
       is_active,
       customer_segment,
       marketing_consent
from "appdb"."public"."customers"
  );