
-- Incremental Mart model for crypto trade analytics
with staging as (

    select * from {{ ref('stg_crypto_trades') }}

)

select
    trade_id,
    product_id,
    side,
    price,
    size,
    trade_time,
    ingestion_time,
    (price * size) as usd_volume
from staging

{% if is_incremental() %}
    -- Filter to process only trade records newer than the max trade_time currently in Snowflake
    where
        trade_time > (select coalesce(max(trade_time), '1970-01-01'::timestamp_tz) from {{ this }})
{% endif %}
