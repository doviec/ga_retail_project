{{ config(materialized='table') }}

-- Country ↔ Currency from ext mapping (avoid scanning GA)
select distinct
  initcap(country)        as country,
  currency                as local_currency
from {{ source('ext', 'country_currency') }}
where country is not null
  and currency is not null
