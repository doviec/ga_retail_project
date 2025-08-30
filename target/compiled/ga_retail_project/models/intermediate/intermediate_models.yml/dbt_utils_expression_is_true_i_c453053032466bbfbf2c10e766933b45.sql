



select
    1
from `e2e-dbt-project`.`analytics_int`.`int_sessions_enriched`

where not(revenue_local revenue_local >= 0 or revenue_local is null)

