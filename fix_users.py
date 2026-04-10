"""
fix_users.py — one-time migration script.
Run from the project root on the Render shell: python fix_users.py
Delete after confirming success.
"""
import json
import os

DATA_DIR = os.environ.get("RENDER_DISK_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"))
USERS_FILE = os.path.join(DATA_DIR, "users.json")

UID = "7213914930"

INVESTED = ["VOO", "RVI", "CRCL", "NVDA", "BTC-USD", "ETH-USD", "SOL-USD"]
WATCHLIST = [
    "ARKG", "ARKQ", "ARKF", "ARKK", "SMH", "XLE",
    "VGT", "VXF", "VXUS", "IVES", "AAPL", "AVBO", "GOOG", "META",
]

with open(USERS_FILE) as f:
    users = json.load(f)

if UID not in users:
    print(f"ERROR: user {UID} not found in {USERS_FILE}")
    exit(1)

user = users[UID]

# Reuse any already-enriched entries (profile, sector, etc.) keyed by ticker
existing = {w["ticker"]: w for w in (
    user.get("watchlist", []) + user.get("invested", []) + user.get("securities", [])
)}

def make_entry(ticker):
    if ticker in existing:
        e = dict(existing[ticker])
        e["ticker"] = ticker  # normalise in case key differed
        return e
    return {
        "ticker": ticker,
        "company_name": ticker,
        "sector": None,
        "asset_class": None,
        "thesis": None,
        "why_added": None,
        "volatility_tier": "medium",
        "date_added": "2026-04-09",
        "status": "holding",
    }

user["invested"]  = [make_entry(t) for t in INVESTED]
user["watchlist"] = [make_entry(t) for t in WATCHLIST]

# Remove old flat list if present
user.pop("securities", None)

with open(USERS_FILE, "w") as f:
    json.dump(users, f, indent=2)

print("Migration complete.")
print("invested: ", [w["ticker"] for w in user["invested"]])
print("watchlist:", [w["ticker"] for w in user["watchlist"]])
