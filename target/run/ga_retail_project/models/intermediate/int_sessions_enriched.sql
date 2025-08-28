

  create or replace view `e2e-dbt-project`.`analytics_int`.`int_sessions_enriched`
  OPTIONS()
  as 

with sessions as (
    select *
    from `e2e-dbt-project`.`analytics_stg`.`stg_sessions_with_fx`
),

hits as (
    select *
    from `e2e-dbt-project`.`analytics_stg`.`stg_hits_products`
)

select
    s.session_date,
    s.visitor_id,
    s.visit_id,
    s.country,
    s.traffic_source,
    s.traffic_medium,
    s.traffic_campaign,
    s.visits,
    s.transactions,
    s.revenue_usd,
    s.currency,
    s.revenue_local,
    h.transaction_id,
    h.product_sku,
    h.product_name,
    h.product_category,
    h.product_quantity,
    h.product_price_usd
from sessions s
left join hits h
  on s.visit_id = h.visit_id
 and s.visitor_id = h.visitor_id;

