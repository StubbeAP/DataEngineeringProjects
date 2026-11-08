"""Backward-compatibility module shim.
Primary ingestion code is located in src.ingestion.kafka_producer.
"""
from src.ingestion.kafka_producer import CryptoKafkaProducer

__all__ = ["CryptoKafkaProducer"]
