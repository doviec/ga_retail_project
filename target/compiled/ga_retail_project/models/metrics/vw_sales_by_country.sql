

select
  country,
  session_date                                        as dt,
  sum(revenue_usd)                                    as revenue_usd,
  sum(revenue_local)                                  as revenue_local,
  count(distinct transaction_id)                      as orders,
  safe_divide(sum(revenue_usd), count(distinct transaction_id)) as aov_usd
from e2e-dbt-project.analytics_mart.fct_sales_item
group by country, dt