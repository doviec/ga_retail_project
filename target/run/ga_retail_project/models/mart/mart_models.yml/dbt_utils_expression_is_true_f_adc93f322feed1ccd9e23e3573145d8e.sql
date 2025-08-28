
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `e2e-dbt-project`.`analytics_mart`.`fct_sales_item`

where not(product_price_usd product_price_usd >= 0 or product_price_usd is null)


  
  
      
    ) dbt_internal_test