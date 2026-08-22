import pytest
from pydantic import ValidationError
from src.producer.websocket_client import TradePayload

def test_valid_payload():
    data = {
        "trade_id": 12345,
        "product_id": "BTC-USD",
        "price": 50000.5,
        "size": 0.1,
        "side": "buy",
        "trade_time": "2024-01-01T00:00:00.000000Z",
        "ingestion_time": "2024-01-01T00:00:00.000000Z"
    }
    payload = TradePayload(**data)
    assert payload.trade_id == 12345
    assert payload.price == 50000.5
    assert payload.size == 0.1

def test_invalid_payload():
    data = {
        "trade_id": "not_an_int",
        "product_id": "BTC-USD",
        "price": 50000.5,
        "size": 0.1,
        "side": "buy",
        "trade_time": "2024-01-01T00:00:00.000000Z",
        "ingestion_time": "2024-01-01T00:00:00.000000Z"
    }
    with pytest.raises(ValidationError):
        TradePayload(**data)
