
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select currency
from `e2e-dbt-project`.`ext`.`country_currency`
where currency is null



  
  
      
    ) dbt_internal_test