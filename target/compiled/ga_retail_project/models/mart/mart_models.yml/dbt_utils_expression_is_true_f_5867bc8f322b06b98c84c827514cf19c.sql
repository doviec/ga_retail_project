



select
    1
from `e2e-dbt-project`.`analytics_mart`.`fct_sales_item`

where not(revenue_local (COALESCE(revenue_local, 0) >= 0))

