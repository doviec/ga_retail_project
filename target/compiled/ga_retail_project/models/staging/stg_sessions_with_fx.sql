with base as (
    select *
    from `e2e-dbt-project`.`analytics_stg`.`stg_sessions`
),
ccy as (
    select country, currency
    from `e2e-dbt-project`.`ext`.`country_currency`
),
fx as (
    select rate_date, currency, rate_to_usd
    from `e2e-dbt-project`.`ext`.`fx_rates`
)
select
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
    case
        when b.revenue_usd is null then null
        when c.currency = 'USD' then b.revenue_usd
        else b.revenue_usd / nullif(f.rate_to_usd, 0)
    end as revenue_local
from base b
left join ccy c
  on c.country = b.country
left join fx f
  on f.rate_date = b.session_date and f.currency = c.currency