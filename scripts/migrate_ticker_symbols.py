"""
scripts/migrate_ticker_symbols.py

One-time migration script — run manually once, then delete.

Usage:
    python scripts/migrate_ticker_symbols.py

For each user's watchlist entry that does not already have a yf_symbol field,
resolves the ticker using the yfinance Lookup API and writes the resolved fields
(yf_symbol, asset_type, finnhub_symbol) back into data/users.json.
"""

import json
import os
import sys
from pathlib import Path

# Ensure the project root is on the path so local modules resolve correctly.
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

import finnhub
from utils.ticker_resolver import resolve_ticker

USERS_FILE = PROJECT_ROOT / "data" / "users.json"


def _get_client() -> finnhub.Client:
    api_key = os.environ.get("FINNHUB_API_KEY", "")
    return finnhub.Client(api_key=api_key)


def main() -> None:
    if not USERS_FILE.exists():
        print(f"ERROR: {USERS_FILE} not found.")
        sys.exit(1)

    with open(USERS_FILE) as f:
        users: dict = json.load(f)

    client = _get_client()
    total_migrated = 0
    total_users = 0

    for uid, user in users.items():
        watchlist: list[dict] = user.get("watchlist", [])
        user_migrated = 0

        for entry in watchlist:
            if "yf_symbol" in entry:
                continue  # already resolved

            ticker = entry["ticker"]
            resolved = resolve_ticker(ticker, client)

            entry["yf_symbol"] = resolved["yf_symbol"]
            entry["asset_type"] = resolved["asset_type"]
            entry["finnhub_symbol"] = resolved["finnhub_symbol"]

            print(
                f"[MIGRATED] {ticker} → {resolved['yf_symbol']} "
                f"({resolved['asset_type']})"
            )
            user_migrated += 1

        if user_migrated:
            total_migrated += user_migrated
            total_users += 1

    with open(USERS_FILE, "w") as f:
        json.dump(users, f, indent=2)

    print(
        f"\nMigration complete: {total_migrated} tickers resolved "
        f"for {total_users} user(s)."
    )


if __name__ == "__main__":
    main()
