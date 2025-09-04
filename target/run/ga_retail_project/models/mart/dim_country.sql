
  
    

    create or replace table e2e-dbt-project.analytics_mart.dim_country
      
    
    

    
    OPTIONS()
    as (
      

-- Country ↔ Currency from ext mapping (avoid scanning GA)
select distinct
  initcap(country)        as country,
  currency                as local_currency
from `e2e-dbt-project`.`ext`.`country_currency`
where country is not null
  and currency is not null
    );
  