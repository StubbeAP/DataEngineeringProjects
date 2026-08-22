import json
from confluent_kafka import Producer
from src.common.logger import get_logger

logger = get_logger(__name__)

class CryptoKafkaProducer:
    def __init__(self, bootstrap_servers: str, client_id: str, topic: str):
        self.topic = topic
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': client_id,
            'acks': 'all'
        }
        self.producer = Producer(conf)
        logger.info(f"Initialized Kafka Producer to {bootstrap_servers}, topic: {topic}")

    def delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Message delivery failed: {err}")

    def produce(self, key: str, value: dict):
        try:
            self.producer.produce(
                topic=self.topic,
                key=key,
                value=json.dumps(value),
                callback=self.delivery_report
            )
            self.producer.poll(0)
        except Exception as e:
            logger.error(f"Error producing message: {e}")

    def flush(self):
        logger.info("Flushing Kafka producer...")
        self.producer.flush()
