

select
  product_sku,
  product_name,
  product_category,
  session_date                                        as dt,
  sum(revenue_usd)                                    as revenue_usd,
  sum(revenue_local)                                  as revenue_local,
  sum(product_quantity)                               as units,
  count(distinct transaction_id)                      as orders
from e2e-dbt-project.analytics_mart.fct_sales_item
group by product_sku, product_name, product_category, dt