
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select dt
from e2e-dbt-project.analytics_mart.dim_date
where dt is null



  
  
      
    ) dbt_internal_test