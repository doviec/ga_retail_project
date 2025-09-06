
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select product_category
from e2e-dbt-project.analytics_stg.product_categories
where product_category is null



  
  
      
    ) dbt_internal_test