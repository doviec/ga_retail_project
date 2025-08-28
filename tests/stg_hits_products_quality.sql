-- Fail if product quantity is negative
select *
from {{ ref('stg_hits_products') }}
where product_quantity < 0

UNION ALL

-- Fail if product price is negative
select *
from {{ ref('stg_hits_products') }}
where product_price_usd < 0
