
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select visitor_id
from `e2e-dbt-project`.`analytics_int`.`int_sessions_enriched`
where visitor_id is null



  
  
      
    ) dbt_internal_test