
    
    

with dbt_test__target as (

  select visit_id as unique_field
  from `e2e-dbt-project`.`stg`.`stg_sessions`
  where visit_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


