
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select currency
from `e2e-dbt-project`.`ext`.`fx_rates`
where currency is null



  
  
      
    ) dbt_internal_test