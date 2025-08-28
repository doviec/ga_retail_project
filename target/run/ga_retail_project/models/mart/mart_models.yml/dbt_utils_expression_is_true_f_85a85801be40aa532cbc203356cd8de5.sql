
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `e2e-dbt-project`.`analytics_mart`.`fct_sales_item`

where not(revenue_usd revenue_usd >= 0 OR revenue_usd IS NULL)


  
  
      
    ) dbt_internal_test