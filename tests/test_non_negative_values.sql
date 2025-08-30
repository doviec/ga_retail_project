-- tests/test_non_negative_values.sql
select *
from {{ ref('fct_sales_item') }}
where product_price_usd < 0
   or revenue_usd < 0
   or revenue_local < 0
