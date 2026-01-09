select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from "appdb"."public_dbt_test__audit"."relationships_stg_orders_96411fe0c89b49c3f4da955dfd358ba0"
    
      
    ) dbt_internal_test