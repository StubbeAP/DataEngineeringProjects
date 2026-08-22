import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

import json
import duckdb
from confluent_kafka import Consumer, KafkaError
from src.common.config import load_config
from src.common.logger import get_logger

logger = get_logger(__name__)

def main():
    config = load_config()
    kafka_config = config['kafka']
    topic = kafka_config['topic']
    
    # Initialize DuckDB
    db_path = "crypto_trades.duckdb"
    conn = duckdb.connect(db_path)
    
    # Create table if not exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crypto_trades (
            trade_id BIGINT,
            product_id VARCHAR,
            price DOUBLE,
            size DOUBLE,
            side VARCHAR,
            trade_time TIMESTAMP,
            ingestion_time TIMESTAMP
        )
    """)
    logger.info(f"Initialized DuckDB at {db_path}")

    # Initialize Consumer
    conf = {
        'bootstrap.servers': kafka_config['bootstrap.servers'],
        'group.id': 'local-duckdb-consumer',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    
    logger.info(f"Subscribed to topic {topic}. Waiting for messages...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(msg.error())
                    break
            
            # Parse message
            try:
                val = json.loads(msg.value().decode('utf-8'))
                
                # Insert into DuckDB
                conn.execute("""
                    INSERT INTO crypto_trades VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    val['trade_id'],
                    val['product_id'],
                    val['price'],
                    val['size'],
                    val['side'],
                    val['trade_time'],
                    val['ingestion_time']
                ))
                
                logger.info(f"Inserted trade {val['trade_id']} for {val['product_id']} into DuckDB")
            except Exception as e:
                logger.error(f"Failed to process message: {e}")
                
    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    finally:
        consumer.close()
        conn.close()

if __name__ == "__main__":
    main()
