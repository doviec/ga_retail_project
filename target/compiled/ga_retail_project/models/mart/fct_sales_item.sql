

with base as (
  select
    session_date,
    visitor_id,
    visit_id,
    country,
    traffic_source,
    traffic_medium,
    traffic_campaign,
    transaction_id,
    product_sku,
    product_name,
    product_category,
    product_quantity,
    product_price_usd,
    revenue_usd,
    revenue_local
  from e2e-dbt-project.analytics_int.int_sessions_enriched
  -- Keep only true sales lines
  where transaction_id is not null
    and coalesce(revenue_usd, 0) > 0
    and coalesce(product_quantity, 0) > 0
)

select * from base

