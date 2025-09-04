{{ config(materialized='view') }}

with cc as (
  select country, currency
  from {{ source('ext', 'country_currency') }}
),
fx as (
  select rate_date, currency, rate_to_usd
  from {{ source('ext', 'fx_rates') }}
),
s as (
  select *
  from {{ ref('stg_sessions') }}
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
  s.revenue_usd as session_revenue_usd_raw,  -- may be null
  cc.currency    as local_currency,
  fx.rate_to_usd
from s
left join cc
  on lower(cc.country) = lower(s.country)
left join fx
  on fx.rate_date = s.session_date
 and fx.currency  = cc.currency
