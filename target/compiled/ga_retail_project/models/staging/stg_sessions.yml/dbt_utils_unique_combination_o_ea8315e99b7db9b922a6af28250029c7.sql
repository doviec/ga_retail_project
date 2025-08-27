





with validation_errors as (

    select
        visitor_id, visit_id
    from `e2e-dbt-project`.`stg`.`stg_sessions`
    group by visitor_id, visit_id
    having count(*) > 1

)

select *
from validation_errors


