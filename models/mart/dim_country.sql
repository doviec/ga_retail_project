-- models/mart/dim_country.sql
WITH countries AS (
    SELECT
        DISTINCT country,
        currency
    FROM {{ ref('stg_sessions_with_fx') }}
)

SELECT * FROM countries
