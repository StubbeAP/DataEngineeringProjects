import urllib.request
import json
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import duckdb
from src.kafka.logger import get_logger

logger = get_logger(__name__)

class CoinbaseApiExtractor:
    """REST API Extractor for Coinbase public market data.
    
    Provides idempotent batch extraction to recover missing trades or backfill
    without relying solely on long-lived WebSocket streaming sessions.
    """

    BASE_URL = "https://api.exchange.coinbase.com"

    def __init__(self, product_ids: Optional[List[str]] = None, db_path: str = "crypto_trades.duckdb"):
        self.product_ids = product_ids or ["BTC-USD", "ETH-USD"]
        self.db_path = db_path
        self._init_duckdb()

    def _init_duckdb(self):
        conn = duckdb.connect(self.db_path)
        conn.execute("""
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
        conn.close()

    def fetch_recent_trades(self, product_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Fetch latest trades from Coinbase REST endpoint."""
        url = f"{self.BASE_URL}/products/{product_id}/trades?limit={limit}"
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "CryptoDataPipeline/1.0", "Accept": "application/json"}
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                if response.status == 200:
                    trades = json.loads(response.read().decode('utf-8'))
                    return trades
                else:
                    logger.error(f"API request failed with status {response.status}")
                    return []
        except Exception as e:
            logger.error(f"Error fetching trades for {product_id}: {e}")
            return []

    def extract_and_load(self, limit: int = 100) -> int:
        """Extract trades from REST API and idempotently insert into DuckDB bronze layer."""
        conn = duckdb.connect(self.db_path)
        total_inserted = 0
        now_iso = datetime.now(timezone.utc).isoformat()

        for product_id in self.product_ids:
            trades = self.fetch_recent_trades(product_id, limit=limit)
            logger.info(f"Fetched {len(trades)} trades from REST API for {product_id}")

            for trade in trades:
                try:
                    conn.execute("""
                        INSERT INTO crypto_trades VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (trade_id) DO NOTHING
                    """, (
                        int(trade['trade_id']),
                        product_id,
                        float(trade['price']),
                        float(trade['size']),
                        trade['side'],
                        trade['time'],
                        now_iso
                    ))
                    total_inserted += 1
                except Exception as e:
                    logger.warning(f"Skipping record due to parse error: {e}")

        conn.close()
        logger.info(f"Completed API extraction. Total records processed/inserted: {total_inserted}")
        return total_inserted

def main():
    extractor = CoinbaseApiExtractor()
    extractor.extract_and_load(limit=50)

if __name__ == "__main__":
    main()
