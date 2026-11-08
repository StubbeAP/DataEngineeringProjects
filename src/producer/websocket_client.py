"""Backward-compatibility module shim.
Primary ingestion code is located in src.ingestion.websocket_client.
"""
from src.ingestion.websocket_client import CoinbaseWebsocketClient, TradePayload

__all__ = ["CoinbaseWebsocketClient", "TradePayload"]
