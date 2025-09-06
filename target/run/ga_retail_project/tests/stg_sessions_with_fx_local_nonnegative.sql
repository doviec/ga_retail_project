
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Check session_revenue_usd_raw is non-negative (when present), and FX is positive
with t as (
  select *
  from e2e-dbt-project.analytics_stg.stg_sessions_with_fx
)
select *
from t
where (session_revenue_usd_raw is not null and session_revenue_usd_raw < 0)
   or (rate_to_usd is not null and rate_to_usd <= 0)
  
  
      
    ) dbt_internal_test