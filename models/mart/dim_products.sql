{{ config(materialized='table') }}

with base as (
  select
    product_sku,
    any_value(product_name)     as product_name,
    any_value(product_category) as product_category
  from {{ ref('fct_sales_item') }}
  group by product_sku
)
select * from base
