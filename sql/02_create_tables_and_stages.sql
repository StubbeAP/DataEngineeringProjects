USE ROLE KAFKA_CONNECTOR_ROLE;
USE DATABASE CRYPTO_DB;
USE SCHEMA PUBLIC;
USE WAREHOUSE CRYPTO_WH;

-- Kafka Connect with the Snowflake Sink Connector typically creates the table automatically
-- if it doesn't exist. However, we can pre-create it to be explicit.
CREATE TABLE IF NOT EXISTS RAW_CRYPTO_TRADES (
    RECORD_METADATA VARIANT,
    RECORD_CONTENT VARIANT
);
