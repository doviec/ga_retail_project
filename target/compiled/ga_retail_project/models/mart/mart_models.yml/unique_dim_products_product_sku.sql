
    
    

with dbt_test__target as (

  select product_sku as unique_field
  from `e2e-dbt-project`.`analytics_mart`.`dim_products`
  where product_sku is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


