
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        visitor_id, visit_id
    from `e2e-dbt-project`.`analytics_stg`.`stg_sessions`
    group by visitor_id, visit_id
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test