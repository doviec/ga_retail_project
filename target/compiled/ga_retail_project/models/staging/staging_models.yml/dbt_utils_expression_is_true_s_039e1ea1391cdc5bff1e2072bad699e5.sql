



select
    1
from `e2e-dbt-project`.`analytics_stg`.`stg_hits_products`

where not(product_price_usd product_price_usd >= 0)

