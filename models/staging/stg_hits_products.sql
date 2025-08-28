with raw as (
    select
        parse_date('%Y%m%d', date)              as hit_date,
        fullVisitorId                           as visitor_id,
        visitId                                 as visit_id,
        hits.transaction.transactionId          as transaction_id,
        p.productSKU                            as product_sku,
        p.v2ProductName                         as product_name,
        p.v2ProductCategory                     as product_category,
        p.productQuantity                       as product_quantity,
        p.productPrice / 1e6                    as product_price_usd
    from `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
         unnest(hits) as hits,
         unnest(hits.product) as p
    where _table_suffix between '20170801' and '20170831'
      and p.productSKU is not null
)
select *
from raw
