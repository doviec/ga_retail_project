





with validation_errors as (

    select
        rate_date, currency
    from `e2e-dbt-project`.`ext`.`fx_rates`
    group by rate_date, currency
    having count(*) > 1

)

select *
from validation_errors


