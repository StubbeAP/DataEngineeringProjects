-- Staging model for crypto trades
with source as (

    select * from {{ source('crypto_source', 'raw_crypto_trades') }}

),

renamed as (

    select
        record_content:trade_id::number as trade_id,
        record_content:product_id::varchar as product_id,
        record_content:price::float as price,
        record_content:size::float as size,
        record_content:side::varchar as side,
        record_content:trade_time::timestamp_tz as trade_time,
        record_content:ingestion_time::timestamp_tz as ingestion_time
    from source
    qualify row_number() over (partition by trade_id order by ingestion_time desc) = 1

)

select * from renamed
