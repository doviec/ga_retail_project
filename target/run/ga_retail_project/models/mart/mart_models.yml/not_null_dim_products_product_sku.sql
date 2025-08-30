
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select product_sku
from `e2e-dbt-project`.`analytics_mart`.`dim_products`
where product_sku is null



  
  
      
    ) dbt_internal_test