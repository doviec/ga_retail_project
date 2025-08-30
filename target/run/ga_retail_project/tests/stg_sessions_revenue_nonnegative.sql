
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Fail if any session has a negative revenue (NULLs are allowed)
select *
from `e2e-dbt-project`.`analytics_stg`.`stg_sessions`
where revenue_usd is not null
  and revenue_usd < 0
  
  
      
    ) dbt_internal_test