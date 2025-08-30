
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select product_sku as from_field
    from `e2e-dbt-project`.`analytics_mart`.`fct_sales_item`
    where product_sku is not null
),

parent as (
    select product_sku as to_field
    from `e2e-dbt-project`.`analytics_stg`.`stg_hits_products`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test