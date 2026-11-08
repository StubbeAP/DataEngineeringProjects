import json
from pathlib import Path
from typing import Optional
import duckdb
from confluent_kafka import Consumer, KafkaError
from src.ingestion.config import load_config
from src.ingestion.logger import get_logger

logger = get_logger(__name__)

class CryptoKafkaConsumer:
    """Consumer for Kafka trade events, persisting into DuckDB bronze layer."""

    def __init__(self, db_path: str = "crypto_trades.duckdb", config_path: str = "config/producer_config.yaml"):
        self.config = load_config(config_path)
        self.kafka_config = self.config['kafka']
        self.topic = self.kafka_config['topic']
        self.db_path = db_path
        self._init_duckdb()

    def _init_duckdb(self):
        self.conn = duckdb.connect(self.db_path)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS crypto_trades (
                trade_id BIGINT PRIMARY KEY,
                product_id VARCHAR,
                price DOUBLE,
                size DOUBLE,
                side VARCHAR,
                trade_time TIMESTAMP,
                ingestion_time TIMESTAMP
            )
        """)
        logger.info(f"Initialized DuckDB bronze table at {self.db_path}")

    def consume(self, max_messages: Optional[int] = None):
        conf = {
            'bootstrap.servers': self.kafka_config['bootstrap.servers'],
            'group.id': 'local-duckdb-consumer',
            'auto.offset.reset': 'earliest'
        }
        consumer = Consumer(conf)
        consumer.subscribe([self.topic])
        logger.info(f"Subscribed to topic {self.topic}. Waiting for messages...")

        count = 0
        try:
            while True:
                if max_messages and count >= max_messages:
                    logger.info(f"Reached max messages limit: {max_messages}")
                    break

                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(msg.error())
                        break

                try:
                    val = json.loads(msg.value().decode('utf-8'))
                    # Idempotent insert with ON CONFLICT DO NOTHING
                    self.conn.execute("""
                        INSERT INTO crypto_trades VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (trade_id) DO NOTHING
                    """, (
                        val['trade_id'],
                        val['product_id'],
                        val['price'],
                        val['size'],
                        val['side'],
                        val['trade_time'],
                        val['ingestion_time']
                    ))
                    count += 1
                    logger.info(f"Inserted trade {val['trade_id']} ({val['product_id']}) into DuckDB")
                except Exception as e:
                    logger.error(f"Failed to process message: {e}")
        except KeyboardInterrupt:
            logger.info("Shutting down consumer...")
        finally:
            consumer.close()
            self.conn.close()

def main():
    consumer = CryptoKafkaConsumer()
    consumer.consume()

if __name__ == "__main__":
    main()
