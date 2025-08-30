-- models/mart/dim_products.sql
WITH products AS (
    SELECT
        product_sku,
        ANY_VALUE(product_name) AS product_name,
        ANY_VALUE(product_category) AS product_category
    FROM {{ ref('stg_hits_products') }}
    WHERE product_sku IS NOT NULL
    GROUP BY product_sku
)

SELECT * FROM products
