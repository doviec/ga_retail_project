



select
    1
from `e2e-dbt-project`.`stg`.`stg_sessions`

where not(revenue_usd revenue_usd >= 0)

