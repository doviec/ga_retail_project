-- models/mart/dim_date.sql
WITH dates AS (
    SELECT
        day AS date_day,
        EXTRACT(YEAR FROM day) AS year,
        EXTRACT(QUARTER FROM day) AS quarter,
        EXTRACT(MONTH FROM day) AS month,
        FORMAT_DATE('%B', day) AS month_name,
        EXTRACT(DAY FROM day) AS day_of_month,
        EXTRACT(DAYOFWEEK FROM day) AS day_of_week,
        FORMAT_DATE('%A', day) AS day_name
    FROM UNNEST(
        GENERATE_DATE_ARRAY('2016-01-01', '2018-12-31', INTERVAL 1 DAY)
    ) AS day
)

SELECT * FROM dates
