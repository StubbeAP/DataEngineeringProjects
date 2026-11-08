"""Ingestion package initialization."""
from src.ingestion.kafka_producer import CryptoKafkaProducer
from src.ingestion.websocket_client import CoinbaseWebsocketClient, TradePayload
from src.ingestion.api_extractor import CoinbaseApiExtractor
from src.ingestion.kafka_consumer import CryptoKafkaConsumer

__all__ = [
    "CryptoKafkaProducer",
    "CoinbaseWebsocketClient",
    "TradePayload",
    "CoinbaseApiExtractor",
    "CryptoKafkaConsumer",
]
