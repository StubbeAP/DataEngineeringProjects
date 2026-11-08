"""Backward-compatibility module shim.
Primary ingestion code is located in src.ingestion.kafka_consumer.
"""
from src.ingestion.kafka_consumer import CryptoKafkaConsumer, main

__all__ = ["CryptoKafkaConsumer", "main"]

if __name__ == "__main__":
    main()
