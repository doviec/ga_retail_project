-- Fail if any computed local revenue is negative (NULLs are fine)
select *
from `e2e-dbt-project`.`analytics_stg`.`stg_sessions_with_fx`
where revenue_local < 0