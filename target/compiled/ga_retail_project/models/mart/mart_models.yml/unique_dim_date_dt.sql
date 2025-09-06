
    
    

with dbt_test__target as (

  select dt as unique_field
  from e2e-dbt-project.analytics_mart.dim_date
  where dt is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


