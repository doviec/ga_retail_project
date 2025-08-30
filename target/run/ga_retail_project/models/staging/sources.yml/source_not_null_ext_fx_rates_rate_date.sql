
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select rate_date
from `e2e-dbt-project`.`ext`.`fx_rates`
where rate_date is null



  
  
      
    ) dbt_internal_test