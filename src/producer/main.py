import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from src.common.config import load_config
from src.common.logger import get_logger
from src.producer.kafka_producer import CryptoKafkaProducer
from src.producer.websocket_client import CoinbaseWebsocketClient

logger = get_logger(__name__)

def main():
    logger.info("Starting Crypto Kafka Producer...")
    config = load_config()

    kafka_config = config['kafka']
    cb_config = config['coinbase']

    producer = CryptoKafkaProducer(
        bootstrap_servers=kafka_config['bootstrap.servers'],
        client_id=kafka_config['client.id'],
        topic=kafka_config['topic']
    )

    ws_client = CoinbaseWebsocketClient(
        url=cb_config['url'],
        product_ids=cb_config['product_ids'],
        channels=cb_config['channels'],
        kafka_producer=producer
    )

    try:
        ws_client.start()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        producer.flush()

if __name__ == "__main__":
    main()
