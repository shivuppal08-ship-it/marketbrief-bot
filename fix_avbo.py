"""
fix_avbo.py — one-time fix: rename AVBO → AVGO in watchlist.
Run: python fix_avbo.py
Delete after confirming success.
"""
import json
import os

DATA_DIR = os.environ.get("RENDER_DISK_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"))
USERS_FILE = os.path.join(DATA_DIR, "users.json")

with open(USERS_FILE) as f:
    users = json.load(f)

changed = False
for uid, user in users.items():
    for entry in user.get("watchlist", []):
        if entry.get("ticker") == "AVBO":
            entry["ticker"] = "AVGO"
            if entry.get("yf_symbol") == "AVBO":
                entry["yf_symbol"] = "AVGO"
            if entry.get("finnhub_symbol") == "AVBO":
                entry["finnhub_symbol"] = "AVGO"
            entry["asset_class"] = None  # force backfill on next restart
            changed = True
            print(f"Renamed AVBO → AVGO for user {uid}")

if changed:
    with open(USERS_FILE, "w") as f:
        json.dump(users, f, indent=2)
    print("Saved.")
else:
    print("AVBO not found — no changes made.")
