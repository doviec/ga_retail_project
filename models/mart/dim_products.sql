{{ config(materialized='table') }}

with src as (
  select
    product_sku,
    any_value(product_name)     as product_name,
    any_value(product_category) as product_category
  from {{ ref('stg_hits_products') }}
  where product_sku is not null
  group by product_sku
)
select * from src
