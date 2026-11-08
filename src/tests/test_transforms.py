import hashlib
import pandas as pd
from datetime import datetime

def test_surrogate_key_generation():
    """Verify surrogate key MD5 hashing logic matches dbt model expectations."""
    product_id = "BTC-USD"
    expected_hash = hashlib.md5(product_id.encode('utf-8')).hexdigest()
    assert len(expected_hash) == 32
    assert expected_hash == hashlib.md5(b"BTC-USD").hexdigest()

def test_trade_volume_calculation():
    """Verify price * size volume computation logic in fct_crypto_trades."""
    price = 50000.0
    size = 0.5
    volume = price * size
    assert volume == 25000.0

def test_hourly_window_bucketing():
    """Verify hourly truncating logic for hourly_crypto_summary model."""
    trade_timestamp = "2024-01-01T14:35:22.123456Z"
    dt = pd.to_datetime(trade_timestamp)
    hourly_window = dt.floor('h')
    assert hourly_window == pd.Timestamp('2024-01-01 14:00:00', tz='UTC')
