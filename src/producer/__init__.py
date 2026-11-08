"""Backward-compatibility package shim.
Primary ingestion code is located in src.ingestion.
"""
from src.ingestion.websocket_client import CoinbaseWebsocketClient, TradePayload
from src.ingestion.kafka_producer import CryptoKafkaProducer

__all__ = ["CoinbaseWebsocketClient", "TradePayload", "CryptoKafkaProducer"]
