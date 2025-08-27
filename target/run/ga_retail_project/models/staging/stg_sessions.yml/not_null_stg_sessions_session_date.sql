
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select session_date
from `e2e-dbt-project`.`stg`.`stg_sessions`
where session_date is null



  
  
      
    ) dbt_internal_test