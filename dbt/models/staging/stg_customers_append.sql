{{
    config(
        materialized='incremental',
        schema='staging',
        incremental_strategy='append'  
    )
}}

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
from {{ source("raw", "customers")}}
{% if is_incremental() %}
WHERE customer_id > (SELECT MAX(customer_id) FROM {{ this }})
{% endif %}

