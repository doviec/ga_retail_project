-- Ensure product price is present & non-negative for rows that should have a price
with t as (
  select *
  from e2e-dbt-project.analytics_stg.stg_hits_products
)
select *
from t
where product_price_local is null
   or product_price_local < 0