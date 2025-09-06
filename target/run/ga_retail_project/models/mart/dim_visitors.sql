
  
    

    create or replace table e2e-dbt-project.analytics_mart.dim_visitors
      
    
    

    
    OPTIONS()
    as (
      

-- Attribute-only visitor dimension. No additive measures here.
with base as (
  select
    cast(visitor_id as string)                                as visitor_id,
    min(session_date)                                         as first_seen_date,
    max(session_date)                                         as last_seen_date,
    any_value(country)                                        as last_country,  -- arbitrary but stable enough
    any_value(traffic_source)                                 as last_source,
    any_value(traffic_medium)                                 as last_medium
  from e2e-dbt-project.analytics_stg.stg_sessions
  group by visitor_id
)

select * from base
    );
  