



select
    1
from `e2e-dbt-project`.`analytics_stg`.`stg_sessions`

where not(revenue_usd revenue_usd >= 0)

