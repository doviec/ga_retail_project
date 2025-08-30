
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select country
from `e2e-dbt-project`.`ext`.`country_currency`
where country is null



  
  
      
    ) dbt_internal_test