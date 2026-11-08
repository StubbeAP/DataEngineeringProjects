"""Backward-compatibility consumer package shim."""
from src.ingestion.kafka_consumer import CryptoKafkaConsumer

__all__ = ["CryptoKafkaConsumer"]
