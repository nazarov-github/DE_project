select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from "appdb"."public_dbt_test__audit"."not_null_stg_orders_order_id"
    
      
    ) dbt_internal_test