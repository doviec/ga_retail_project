WITH base AS (
  SELECT *
  FROM {{ ref('stg_sessions') }}
),
ccy AS (
  SELECT country, currency
  FROM `{{ target.project }}.ext.country_currency`
),
fx AS (
  SELECT rate_date, currency, rate_to_usd
  FROM `{{ target.project }}.ext.fx_rates`
)
SELECT
  b.session_date,
  b.visitor_id,
  b.visit_id,
  b.country,
  b.traffic_source,
  b.traffic_medium,
  b.traffic_campaign,
  b.visits,
  b.transactions,
  b.revenue_usd,
  c.currency,
  CASE
    WHEN b.revenue_usd IS NULL THEN NULL
    WHEN c.currency = 'USD' THEN b.revenue_usd
    ELSE b.revenue_usd / NULLIF(f.rate_to_usd, 0)
  END AS revenue_local
FROM base b
LEFT JOIN ccy c
  ON c.country = b.country
LEFT JOIN fx f
  ON f.rate_date = b.session_date AND f.currency = c.currency
