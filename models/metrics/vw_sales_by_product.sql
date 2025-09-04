{{ config(
  materialized='view',
  schema='mart',
  alias='vw_sales_by_product'
) }}

select
  product_sku,
  product_name,
  product_category,
  session_date                                        as dt,
  sum(revenue_usd)                                    as revenue_usd,
  sum(revenue_local)                                  as revenue_local,
  sum(product_quantity)                               as units,
  count(distinct transaction_id)                      as orders
from {{ ref('fct_sales_item') }}
group by product_sku, product_name, product_category, dt
