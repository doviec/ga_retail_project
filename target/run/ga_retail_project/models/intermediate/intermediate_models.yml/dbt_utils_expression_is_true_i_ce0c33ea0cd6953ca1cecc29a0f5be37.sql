
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `e2e-dbt-project`.`analytics_int`.`int_sessions_enriched`

where not(revenue_usd revenue_usd >= 0 or revenue_usd is null)


  
  
      
    ) dbt_internal_test