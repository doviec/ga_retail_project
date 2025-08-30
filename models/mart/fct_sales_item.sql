{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['transaction_id','product_sku','visit_id'],
    partition_by={'field': 'session_date', 'data_type': 'date'},
    cluster_by=['country','product_category','product_sku']
) }}

with base as (
    select *
    from {{ ref('int_sessions_enriched') }}
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
-- only process new dates vs the max in the target
where session_date >
  (select coalesce(max(session_date), date('1970-01-01')) from {{ this }})
{% endif %}
