

with base as (
  select
    product_sku,
    any_value(product_name)     as product_name,
    any_value(product_category) as product_category
  from e2e-dbt-project.analytics_mart.fct_sales_item
  group by product_sku
)
select * from base