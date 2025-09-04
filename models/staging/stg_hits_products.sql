{{ config(materialized='view') }}

-- Unnest product-level hits over the date window controlled by vars.
with base as (
  select
    parse_date('%Y%m%d', _TABLE_SUFFIX)              as session_date,
    cast(fullVisitorId as string)                    as visitor_id,
    cast(visitId as string)                          as visit_id,
    geoNetwork.country                               as country,
    trafficSource.source                             as traffic_source,
    trafficSource.medium                             as traffic_medium,
    trafficSource.campaign                           as traffic_campaign,
    h.transaction.transactionId                      as transaction_id,
    p.productSKU                                     as product_sku,
    p.v2ProductName                                  as product_name,
    p.v2ProductCategory                              as product_category,
    p.productQuantity                                as product_quantity,
    -- GA prices are in micros in the export; treat as "local" at this stage
    safe_divide(p.productPrice, 1e6)                 as product_price_local,
    safe_divide(h.transaction.transactionRevenue, 1e6) as item_revenue_local
  from `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
       unnest(hits) as h,
       unnest(h.product) as p
  where _TABLE_SUFFIX between '{{ var("ga_start") }}'
                        and '{{ var("ga_end") }}'
    and p.productSKU is not null
)
select * from base
