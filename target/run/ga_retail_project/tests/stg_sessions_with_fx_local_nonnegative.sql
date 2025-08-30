
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Fail if any computed local revenue is negative (NULLs are fine)
select *
from `e2e-dbt-project`.`analytics_stg`.`stg_sessions_with_fx`
where revenue_local < 0
  
  
      
    ) dbt_internal_test