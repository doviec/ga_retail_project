
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        visit_id, product_sku, hit_date
    from `e2e-dbt-project`.`analytics_stg`.`stg_hits_products`
    group by visit_id, product_sku, hit_date
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test