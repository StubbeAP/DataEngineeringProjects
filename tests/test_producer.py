from unittest.mock import MagicMock
from src.producer.websocket_client import CoinbaseWebsocketClient

def test_websocket_on_message_match():
    # Mock producer
    mock_producer = MagicMock()
    
    # Init client
    client = CoinbaseWebsocketClient(
        url="dummy",
        product_ids=["BTC-USD"],
        channels=["matches"],
        kafka_producer=mock_producer
    )
    
    # Valid match message
    message = '{"type":"match","trade_id":123,"maker_order_id":"1","taker_order_id":"2","side":"buy","size":"0.1","price":"50000","product_id":"BTC-USD","sequence":1,"time":"2024-01-01T00:00:00.000000Z"}'
    
    client.on_message(None, message)
    
    mock_producer.produce.assert_called_once()
    
def test_websocket_on_message_other():
    # Mock producer
    mock_producer = MagicMock()
    
    # Init client
    client = CoinbaseWebsocketClient(
        url="dummy",
        product_ids=["BTC-USD"],
        channels=["matches"],
        kafka_producer=mock_producer
    )
    
    # Non-match message
    message = '{"type":"subscriptions","channels":[{"name":"matches","product_ids":["BTC-USD"]}]}'
    
    client.on_message(None, message)
    
    mock_producer.produce.assert_not_called()
