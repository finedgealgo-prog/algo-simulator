"""
create_index.py
---------------
One-time setup script: Creates a MongoDB index on `timestamp` and `expiry`
in the option_chain_historical_data collection. Run once to enable fast range queries.

Usage:
    python -m mini_strangle.create_index
"""
from pymongo import ASCENDING, MongoClient
import time


def create_index(mongo_uri: str = "mongodb://localhost:27017/") -> None:
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    col = client["stock_data"]["option_chain_historical_data"]

    print("Existing indices:")
    for idx in col.list_indexes():
        print(" ", idx)

    print("\nCreating index on (timestamp ASC, expiry ASC)...")
    t0 = time.time()
    col.create_index(
        [("timestamp", ASCENDING), ("expiry", ASCENDING)],
        background=False,   # foreground = faster for a one-time setup
    )
    print(f"Done in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    create_index()
