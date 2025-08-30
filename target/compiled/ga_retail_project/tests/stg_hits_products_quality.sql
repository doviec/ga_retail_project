-- Fail if product quantity is negative
select *
from `e2e-dbt-project`.`analytics_stg`.`stg_hits_products`
where product_quantity < 0

UNION ALL

-- Fail if product price is negative
select *
from `e2e-dbt-project`.`analytics_stg`.`stg_hits_products`
where product_price_usd < 0