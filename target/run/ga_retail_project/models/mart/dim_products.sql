
  
    

    create or replace table `e2e-dbt-project`.`analytics_mart`.`dim_products`
      
    
    

    
    OPTIONS()
    as (
      -- models/mart/dim_products.sql
WITH products AS (
    SELECT
        product_sku,
        ANY_VALUE(product_name) AS product_name,
        ANY_VALUE(product_category) AS product_category
    FROM `e2e-dbt-project`.`analytics_stg`.`stg_hits_products`
    WHERE product_sku IS NOT NULL
    GROUP BY product_sku
)

SELECT * FROM products
    );
  