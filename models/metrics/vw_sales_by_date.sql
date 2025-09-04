{{ config(
  materialized='view',
  schema='mart',
  alias='vw_sales_by_date'
) }}

select
  session_date                                        as dt,
  sum(revenue_usd)                                    as revenue_usd,
  sum(revenue_local)                                  as revenue_local,
  count(distinct transaction_id)                      as orders,
  count(distinct visitor_id)                          as visitors,
  safe_divide(sum(revenue_usd), count(distinct transaction_id)) as aov_usd
from {{ ref('fct_sales_item') }}
group by dt
