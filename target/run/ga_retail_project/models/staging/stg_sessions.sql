

  create or replace view `e2e-dbt-project`.`stg`.`stg_sessions`
  OPTIONS()
  as WITH source AS (
  SELECT
    PARSE_DATE('%Y%m%d', date)                  AS session_date,
	geoNetwork.country                         as country,
    fullVisitorId                               AS visitor_id,
    visitId                                     AS visit_id,
    trafficSource.source                        AS traffic_source,
    trafficSource.medium                        AS traffic_medium,
    trafficSource.campaign                      AS traffic_campaign,
    totals.visits                               AS visits,
    totals.transactions                         AS transactions,
    SAFE_DIVIDE(totals.transactionRevenue, 1e6) AS revenue_usd
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE _TABLE_SUFFIX BETWEEN '20170801' AND '20170831'
)
SELECT * FROM source;

