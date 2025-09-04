

-- Simple, robust date dimension built from observed session dates
select
  session_date as dt
from e2e-dbt-project.analytics_stg.stg_sessions
where session_date is not null
group by session_date