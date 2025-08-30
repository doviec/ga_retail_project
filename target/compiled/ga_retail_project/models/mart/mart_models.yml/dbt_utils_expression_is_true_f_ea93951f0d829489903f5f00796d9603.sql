



select
    1
from `e2e-dbt-project`.`analytics_mart`.`fct_sales_item`

where not(revenue_usd (CAST(COALESCE(`revenue_usd`, 0) AS FLOAT64) >= 0))

