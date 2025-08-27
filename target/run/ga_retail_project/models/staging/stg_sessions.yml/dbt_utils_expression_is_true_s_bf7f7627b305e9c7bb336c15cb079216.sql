
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `e2e-dbt-project`.`stg`.`stg_sessions`

where not(revenue_usd revenue_usd is null OR revenue_usd >= 0)


  
  
      
    ) dbt_internal_test