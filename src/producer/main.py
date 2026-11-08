"""Backward-compatibility entrypoint shim.
Primary ingestion code is located in src.ingestion.main.
"""
from src.ingestion.main import main

if __name__ == "__main__":
    main()
