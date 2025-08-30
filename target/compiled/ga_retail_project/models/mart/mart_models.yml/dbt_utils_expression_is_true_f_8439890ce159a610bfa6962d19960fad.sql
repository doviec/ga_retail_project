



select
    1
from `e2e-dbt-project`.`analytics_mart`.`fct_sales_item`

where not(product_price_usd (COALESCE(product_price_usd, 0) >= 0))

