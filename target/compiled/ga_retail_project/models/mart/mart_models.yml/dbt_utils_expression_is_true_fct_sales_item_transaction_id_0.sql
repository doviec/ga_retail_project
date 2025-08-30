



select
    1
from `e2e-dbt-project`.`analytics_mart`.`fct_sales_item`

where not(transaction_id >= 0)

