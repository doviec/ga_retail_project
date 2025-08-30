-- tests/test_non_negative_values.sql
select *
from `e2e-dbt-project`.`analytics_mart`.`fct_sales_item`
where product_price_usd < 0
   or revenue_usd < 0
   or revenue_local < 0