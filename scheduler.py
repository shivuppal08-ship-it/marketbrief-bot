"""
scheduler.py
GitHub Actions entry point.
Runs every 30 minutes on weekdays (UTC 11:00–23:00).

For each user whose briefing_time falls in the current 30-minute window
(and who hasn't already received a briefing today), calls
generate_briefing_for_user().
"""

import asyncio
import json
import logging
import os
from datetime import datetime, date

import pytz
from dotenv import load_dotenv

from briefing import generate_briefing_for_user

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

DATA_DIR = os.environ.get(
    "RENDER_DISK_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data"),
)
USERS_FILE = os.path.join(DATA_DIR, "users.json")


def load_users() -> dict:
    if not os.path.exists(USERS_FILE):
        return {}
    with open(USERS_FILE) as f:
        return json.load(f)


def _user_is_eligible(user: dict, now_utc: datetime) -> bool:
    """
    Returns True if this user should receive a briefing right now.

    Conditions:
    1. Onboarding is complete.
    2. User has at least one ticker in their watchlist.
    3. Today (in the user's local timezone) is a weekday (Mon–Fri).
    4. The current UTC time falls within the user's 30-minute send window.
       briefing_time is stored as "HH:MM" in the user's local timezone.
    5. The user has not already received a briefing today (last_briefing_date != today).
    """
    if not user.get("onboarding_complete"):
        return False
    if not user.get("watchlist"):
        return False

    tz_str = user.get("timezone", "UTC")
    try:
        tz = pytz.timezone(tz_str)
    except pytz.UnknownTimeZoneError:
        tz = pytz.utc

    now_local = now_utc.astimezone(tz)

    # Skip weekends
    if now_local.weekday() >= 5:
        return False

    # Already sent today?
    today_str = now_local.date().isoformat()
    if user.get("last_briefing_date") == today_str:
        return False

    # Does briefing_time fall in the current 30-minute window?
    briefing_time_str = user.get("briefing_time", "08:00")
    try:
        bh, bm = [int(x) for x in briefing_time_str.split(":")]
    except (ValueError, AttributeError):
        logger.warning(f"Invalid briefing_time '{briefing_time_str}' for user "
                       f"{user.get('telegram_id')} — skipping")
        return False

    current_minutes = now_local.hour * 60 + now_local.minute
    target_minutes = bh * 60 + bm

    # Accept if within the current 30-min slot [target, target+30)
    return target_minutes <= current_minutes < target_minutes + 30


async def run_scheduler() -> None:
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not bot_token:
        logger.error("TELEGRAM_BOT_TOKEN not set — aborting.")
        return

    now_utc = datetime.now(pytz.utc)
    logger.info(f"Scheduler run at {now_utc.strftime('%Y-%m-%d %H:%M UTC')}")

    users = load_users()
    eligible = [u for u in users.values() if _user_is_eligible(u, now_utc)]
    logger.info(f"{len(eligible)} user(s) eligible for a briefing this run.")

    if not eligible:
        return

    # Run all eligible briefings concurrently
    tasks = [generate_briefing_for_user(user, bot_token) for user in eligible]
    await asyncio.gather(*tasks, return_exceptions=True)

    logger.info("Scheduler run complete.")


if __name__ == "__main__":
    asyncio.run(run_scheduler())
