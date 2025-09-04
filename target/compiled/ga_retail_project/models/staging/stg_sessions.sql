

-- Pull sessions over a configurable date window using _TABLE_SUFFIX
-- Usage example:
--   dbt run --select stg_sessions --vars '{ga_start: "20170801", ga_end: "20170831"}'

with src as (
  select
    -- Drive the session date from the shard suffix, not the row field
    parse_date('%Y%m%d', _TABLE_SUFFIX)                as session_date,
    cast(fullVisitorId as string)                      as visitor_id,
    cast(visitId as string)                            as visit_id,
    geoNetwork.country                                 as country,
    trafficSource.source                               as traffic_source,
    trafficSource.medium                               as traffic_medium,
    trafficSource.campaign                             as traffic_campaign,
    totals.visits                                      as visits,
    totals.transactions                                as transactions,
    safe_divide(totals.transactionRevenue, 1e6)        as revenue_usd   -- may be null
  from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
where _TABLE_SUFFIX between '20170101'
                        and '20171231'
)

select *
from src