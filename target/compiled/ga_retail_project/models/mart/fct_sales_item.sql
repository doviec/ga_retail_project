

with base as (
    select *
    from `e2e-dbt-project`.`analytics_int`.`int_sessions_enriched`
)

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
from base