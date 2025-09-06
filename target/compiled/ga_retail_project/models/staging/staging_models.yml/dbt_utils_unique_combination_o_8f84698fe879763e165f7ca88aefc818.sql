





with validation_errors as (

    select
        visitor_id, visit_id, session_date
    from e2e-dbt-project.analytics_stg.stg_sessions
    group by visitor_id, visit_id, session_date
    having count(*) > 1

)

select *
from validation_errors


