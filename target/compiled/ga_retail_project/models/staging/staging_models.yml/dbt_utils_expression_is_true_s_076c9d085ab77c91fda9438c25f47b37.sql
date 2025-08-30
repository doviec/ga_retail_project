



select
    1
from `e2e-dbt-project`.`analytics_stg`.`stg_sessions_with_fx`

where not(revenue_local revenue_local >= 0 or revenue_local is null)

