-- Fail if any session has a negative revenue (NULLs are allowed)
select *
from `e2e-dbt-project`.`analytics_stg`.`stg_sessions`
where revenue_usd is not null
  and revenue_usd < 0