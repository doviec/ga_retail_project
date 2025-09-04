{{ config(
    materialized='incremental',
    unique_key=['session_date','visitor_id','visit_id','transaction_id','product_sku'],
    partition_by={'field': 'session_date', 'data_type': 'date'},
    cluster_by=['country','product_sku','product_category']
) }}

with base as (
  select * from {{ ref('int_sessions_enriched') }}
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

{% if is_incremental() %}
  -- Only load new days on incremental runs
  where session_date > (select ifnull(max(session_date), date('1980-01-01')) from {{ this }})
{% endif %}
