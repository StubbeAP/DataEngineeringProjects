import duckdb

def main():
    db_path = "crypto_trades.duckdb"
    try:
        # Try connecting in read_only mode first
        conn = duckdb.connect(db_path, read_only=True)
    except Exception:
        conn = duckdb.connect(db_path)

    print("\n=== Trade Summary ===")
    summary = conn.execute("""
        SELECT product_id, COUNT(*) AS total_trades, ROUND(AVG(price), 2) AS avg_price 
        FROM crypto_trades 
        GROUP BY product_id
    """).df()
    print(summary.to_string(index=False))

    print("\n=== Latest 10 Trades ===")
    trades = conn.execute("""
        SELECT trade_id, product_id, price, size, side, trade_time 
        FROM crypto_trades 
        ORDER BY trade_time DESC 
        LIMIT 10
    """).df()
    print(trades.to_string(index=False))

    conn.close()

if __name__ == "__main__":
    main()
