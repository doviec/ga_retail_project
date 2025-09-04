

-- Silver: join item-level hits with session-day FX, compute USD metrics.
with sess as (
  select *
  from e2e-dbt-project.analytics_stg.stg_sessions_with_fx
),
items as (
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
    -- local currency amounts from staging
    product_price_local,
    item_revenue_local
  from e2e-dbt-project.analytics_stg.stg_hits_products
),
joined as (
  select
    i.session_date,
    i.visitor_id,
    i.visit_id,
    i.country,
    s.traffic_source,
    s.traffic_medium,
    s.traffic_campaign,
    i.transaction_id,
    i.product_sku,
    i.product_name,
    i.product_category,
    i.product_quantity,
    i.product_price_local,
    coalesce(i.item_revenue_local, i.product_quantity * i.product_price_local) as revenue_local,
    -- FX conversion at session_date granularity
    (i.product_price_local * s.rate_to_usd) as product_price_usd,
    (coalesce(i.item_revenue_local, i.product_quantity * i.product_price_local) * s.rate_to_usd) as revenue_usd,
    s.rate_to_usd,
    s.local_currency
  from items i
  left join sess s
    on s.session_date = i.session_date
   and s.visitor_id   = i.visitor_id
   and s.visit_id     = i.visit_id
   and lower(s.country) = lower(i.country)
)

select * from joined