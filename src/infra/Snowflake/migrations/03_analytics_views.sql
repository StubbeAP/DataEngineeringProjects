USE ROLE ACCOUNTADMIN;
USE DATABASE CRYPTO_DB;
USE SCHEMA PUBLIC;

-- Create a view that flattens the JSON from RECORD_CONTENT
CREATE OR REPLACE VIEW V_CRYPTO_TRADES AS
SELECT
    RECORD_CONTENT:trade_id::NUMBER AS trade_id,
    RECORD_CONTENT:product_id::VARCHAR AS product_id,
    RECORD_CONTENT:price::FLOAT AS price,
    RECORD_CONTENT:size::FLOAT AS size,
    RECORD_CONTENT:side::VARCHAR AS side,
    RECORD_CONTENT:trade_time::TIMESTAMP_TZ AS trade_time,
    RECORD_CONTENT:ingestion_time::TIMESTAMP_TZ AS ingestion_time,
    RECORD_METADATA:topic::VARCHAR AS kafka_topic,
    RECORD_METADATA:partition::NUMBER AS kafka_partition,
    RECORD_METADATA:offset::NUMBER AS kafka_offset
FROM RAW_CRYPTO_TRADES;
