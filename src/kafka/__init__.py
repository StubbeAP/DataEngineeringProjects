"""Ingestion package initialization."""
from src.kafka.kafka_producer import CryptoKafkaProducer
from src.kafka.websocket_client import CoinbaseWebsocketClient, TradePayload
from src.kafka.api_extractor import CoinbaseApiExtractor
from src.kafka.kafka_consumer import CryptoKafkaConsumer

__all__ = [
    "CryptoKafkaProducer",
    "CoinbaseWebsocketClient",
    "TradePayload",
    "CoinbaseApiExtractor",
    "CryptoKafkaConsumer",
]
