import json
import time
from datetime import datetime, timezone
import websocket
from pydantic import BaseModel, ValidationError
from src.common.logger import get_logger
from src.producer.kafka_producer import CryptoKafkaProducer

logger = get_logger(__name__)

class TradePayload(BaseModel):
    trade_id: int
    product_id: str
    price: float
    size: float
    side: str
    trade_time: str
    ingestion_time: str

class CoinbaseWebsocketClient:
    def __init__(self, url: str, product_ids: list, channels: list, kafka_producer: CryptoKafkaProducer):
        self.url = url
        self.product_ids = product_ids
        self.channels = channels
        self.kafka_producer = kafka_producer
        self.ws = None

    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            if data.get('type') == 'match':
                # Normalize payload
                payload = TradePayload(
                    trade_id=data['trade_id'],
                    product_id=data['product_id'],
                    price=float(data['price']),
                    size=float(data['size']),
                    side=data['side'],
                    trade_time=data['time'],
                    ingestion_time=datetime.now(timezone.utc).isoformat()
                )
                
                # Produce to Kafka
                self.kafka_producer.produce(
                    key=payload.product_id,
                    value=payload.model_dump()
                )
        except ValidationError as e:
            logger.error(f"Payload validation error: {e}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def on_error(self, ws, error):
        logger.error(f"WebSocket error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        logger.info("WebSocket connection closed")

    def on_open(self, ws):
        logger.info("WebSocket connection opened. Subscribing to channels...")
        subscribe_msg = {
            "type": "subscribe",
            "product_ids": self.product_ids,
            "channels": self.channels
        }
        ws.send(json.dumps(subscribe_msg))

    def start(self):
        websocket.enableTrace(False)
        self.ws = websocket.WebSocketApp(
            self.url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        while True:
            self.ws.run_forever()
            logger.info("Reconnecting in 5 seconds...")
            time.sleep(5)
