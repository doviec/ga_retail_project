-- models/mart/dim_visitors.sql
WITH visitors AS (
    SELECT
        visitor_id,
        COUNT(DISTINCT visit_id) AS total_sessions,
        COUNT(DISTINCT transaction_id) AS total_transactions
    FROM {{ ref('fct_sales_item') }}
    GROUP BY visitor_id
)

SELECT * FROM visitors
