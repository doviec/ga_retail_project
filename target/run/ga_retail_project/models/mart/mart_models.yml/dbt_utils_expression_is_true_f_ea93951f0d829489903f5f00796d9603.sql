
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `e2e-dbt-project`.`analytics_mart`.`fct_sales_item`

where not(revenue_usd (CAST(COALESCE(`revenue_usd`, 0) AS FLOAT64) >= 0))


  
  
      
    ) dbt_internal_test