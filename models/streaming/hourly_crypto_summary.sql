{{ config(
    materialized='incremental',
    unique_key=['hourly_window', 'product_id'],
    incremental_strategy='merge'
) }}

-- Incremental aggregate model for hourly trade volume and metrics
with trades as (

    select * from {{ ref('stg_crypto_trades') }}

)

select
    date_trunc('hour', trade_time) as hourly_window,
    product_id,
    count(trade_id) as total_trades,
    sum(size) as total_crypto_volume,
    sum(price * size) as total_usd_volume,
    avg(price) as avg_price,
    max(ingestion_time) as last_ingested_at
from trades

{% if is_incremental() %}
    -- Reprocess current active hourly window
    where
        trade_time
        >= (select coalesce(max(hourly_window), '1970-01-01'::timestamp_tz) from {{ this }})
{% endif %}

group by 1, 2
