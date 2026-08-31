
with unique_products as (
    select distinct product_id
    from {{ ref('stg_crypto_trades') }}
    where product_id is not null
)

select
    md5(product_id) as product_key,
    product_id,
    split_part(product_id, '-', 1) as base_currency,
    split_part(product_id, '-', 2) as quote_currency,
    case
        when split_part(product_id, '-', 2) in ('USD', 'EUR', 'GBP', 'JPY') then true
        else false
    end as is_fiat_quote
from unique_products
