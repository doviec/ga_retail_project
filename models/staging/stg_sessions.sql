with source as (
    select
        parse_date('%Y%m%d', date)                  as session_date,
        geoNetwork.country                         as country,
        fullVisitorId                               as visitor_id,
        visitId                                     as visit_id,
        trafficSource.source                        as traffic_source,
        trafficSource.medium                        as traffic_medium,
        trafficSource.campaign                      as traffic_campaign,
        totals.visits                               as visits,
        totals.transactions                         as transactions,
        safe_divide(totals.transactionRevenue, 1e6) as revenue_usd
    from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
    where _table_suffix between '20170801' and '20170831'
)
select * from source
