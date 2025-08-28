



select
    1
from `e2e-dbt-project`.`analytics_mart`.`fct_sales_item`

where not(revenue_usd revenue_usd >= 0 or revenue_usd is null)

