select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from "appdb"."public_dbt_test__audit"."unique_int_order_details_order_id"
    
      
    ) dbt_internal_test