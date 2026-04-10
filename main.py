"""
MarketBrief Bot — main.py
Handles all user interactions: onboarding and ongoing commands.
"""

import asyncio
import os
import json
import logging
import re
import tempfile
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

import anthropic
import pytz
from dotenv import load_dotenv

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ConversationHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)

import yfinance as yf

from utils.market_data import validate_tickers_parallel, _get_client
from utils.sheets import fetch_tickers_from_sheets, parse_excel_tickers
from briefing import generate_briefing_for_user

load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

MODEL = "claude-sonnet-4-6"


def _start_health_server() -> None:
    """
    Minimal HTTP server for Render's health-check requirement.
    Binds to PORT env var (default 8080) and responds 200 to GET /.
    Runs in a daemon thread so it doesn't block the Telegram polling loop.
    """
    port = int(os.environ.get("PORT", 8080))

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")

        def do_HEAD(self):
            self.send_response(200)
            self.end_headers()

        def log_message(self, format, *args):
            pass  # suppress per-request access logs

    server = HTTPServer(("0.0.0.0", port), _Handler)
    logger.info(f"Health-check server listening on port {port}")
    server.serve_forever()


def _esc(text: str) -> str:
    """Escape special characters for Telegram MarkdownV2."""
    special = r'\_*[]()~`>#+-=|{}.!'
    return ''.join(f'\\{c}' if c in special else c for c in str(text))

# ---------------------------------------------------------------------------
# Conversation states
# ---------------------------------------------------------------------------
(
    WATCHLIST,
    KNOWLEDGE_LEVEL,
    CONCEPT_FREQ,
    TIMEZONE_TIME,
    GOALS,
    GOALS_CONFIRM,
) = range(6)

DATA_DIR = os.environ.get(
    "RENDER_DISK_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data"),
)
os.makedirs(DATA_DIR, exist_ok=True)
USERS_FILE = os.path.join(DATA_DIR, "users.json")

# One-time seed: if persistent disk has no users.json, copy from repo-local fallback
_REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
_FALLBACK_USERS = os.path.join(_REPO_ROOT, "data", "users.json")
if not os.path.exists(USERS_FILE) and os.path.exists(_FALLBACK_USERS):
    import shutil
    shutil.copy2(_FALLBACK_USERS, USERS_FILE)
    logging.getLogger(__name__).info("SEED: Copied users.json to persistent disk.")

# ---------------------------------------------------------------------------
# Timezone lookup tables
# ---------------------------------------------------------------------------

CITY_TO_TZ: dict[str, str] = {
    "new york": "America/New_York",
    "los angeles": "America/Los_Angeles",
    "la": "America/Los_Angeles",
    "chicago": "America/Chicago",
    "london": "Europe/London",
    "paris": "Europe/Paris",
    "berlin": "Europe/Berlin",
    "tokyo": "Asia/Tokyo",
    "sydney": "Australia/Sydney",
    "dubai": "Asia/Dubai",
    "singapore": "Asia/Singapore",
    "hong kong": "Asia/Hong_Kong",
    "toronto": "America/Toronto",
    "miami": "America/New_York",
    "boston": "America/New_York",
    "seattle": "America/Los_Angeles",
    "san francisco": "America/Los_Angeles",
    "sf": "America/Los_Angeles",
    "denver": "America/Denver",
    "phoenix": "America/Phoenix",
    "dallas": "America/Chicago",
    "houston": "America/Chicago",
    "atlanta": "America/New_York",
    "mumbai": "Asia/Kolkata",
    "delhi": "Asia/Kolkata",
    "bangalore": "Asia/Kolkata",
    "india": "Asia/Kolkata",
    "shanghai": "Asia/Shanghai",
    "beijing": "Asia/Shanghai",
    "china": "Asia/Shanghai",
    "seoul": "Asia/Seoul",
    "amsterdam": "Europe/Amsterdam",
    "zurich": "Europe/Zurich",
    "madrid": "Europe/Madrid",
    "rome": "Europe/Rome",
    "moscow": "Europe/Moscow",
    "istanbul": "Europe/Istanbul",
    "tel aviv": "Asia/Jerusalem",
    "johannesburg": "Africa/Johannesburg",
    "mexico city": "America/Mexico_City",
    "sao paulo": "America/Sao_Paulo",
    "buenos aires": "America/Argentina/Buenos_Aires",
    "vancouver": "America/Vancouver",
    "montreal": "America/Toronto",
    "calgary": "America/Edmonton",
    "auckland": "Pacific/Auckland",
    "new zealand": "Pacific/Auckland",
    "melbourne": "Australia/Melbourne",
    "brisbane": "Australia/Brisbane",
    "perth": "Australia/Perth",
    "australia": "Australia/Sydney",
    "bangkok": "Asia/Bangkok",
    "jakarta": "Asia/Jakarta",
    "manila": "Asia/Manila",
    "kuala lumpur": "Asia/Kuala_Lumpur",
    "karachi": "Asia/Karachi",
    "nairobi": "Africa/Nairobi",
    "lagos": "Africa/Lagos",
    "cairo": "Africa/Cairo",
    "riyadh": "Asia/Riyadh",
    "tehran": "Asia/Tehran",
}

TZ_ABBR_MAP: dict[str, str] = {
    "est": "America/New_York",
    "edt": "America/New_York",
    "cst": "America/Chicago",
    "cdt": "America/Chicago",
    "mst": "America/Denver",
    "mdt": "America/Denver",
    "pst": "America/Los_Angeles",
    "pdt": "America/Los_Angeles",
    "gmt": "UTC",
    "utc": "UTC",
    "bst": "Europe/London",
    "cet": "Europe/Paris",
    "ist": "Asia/Kolkata",
    "jst": "Asia/Tokyo",
    "kst": "Asia/Seoul",
    "hkt": "Asia/Hong_Kong",
    "sgt": "Asia/Singapore",
    "aest": "Australia/Sydney",
    "aedt": "Australia/Sydney",
    "nzst": "Pacific/Auckland",
    "nzdt": "Pacific/Auckland",
    "eat": "Africa/Nairobi",
    "sat": "Africa/Johannesburg",
    "gst": "Asia/Dubai",
    "pkt": "Asia/Karachi",
    "bdt": "Asia/Dhaka",
    "ict": "Asia/Bangkok",
    "wib": "Asia/Jakarta",
    "nst": "America/St_Johns",
    "ast": "America/Halifax",
    "hst": "Pacific/Honolulu",
    "akst": "America/Anchorage",
    "art": "America/Argentina/Buenos_Aires",
    "brt": "America/Sao_Paulo",
    "eet": "Europe/Helsinki",
    "msk": "Europe/Moscow",
    "trt": "Europe/Istanbul",
    "irst": "Asia/Tehran",
}

# UTC whole-hour offset (integer) → canonical pytz TZ name
_HOUR_OFFSET_TO_TZ: dict[int, str] = {
    -12: "Etc/GMT+12",
    -11: "Etc/GMT+11",
    -10: "Pacific/Honolulu",
    -9: "America/Anchorage",
    -8: "America/Los_Angeles",
    -7: "America/Denver",
    -6: "America/Chicago",
    -5: "America/New_York",
    -4: "America/Halifax",
    -3: "America/Sao_Paulo",
    -2: "Etc/GMT+2",
    -1: "Atlantic/Azores",
    0: "UTC",
    1: "Europe/Paris",
    2: "Europe/Helsinki",
    3: "Europe/Moscow",
    4: "Asia/Dubai",
    5: "Asia/Karachi",
    6: "Asia/Dhaka",
    7: "Asia/Bangkok",
    8: "Asia/Shanghai",
    9: "Asia/Tokyo",
    10: "Australia/Sydney",
    11: "Pacific/Noumea",
    12: "Pacific/Auckland",
    13: "Pacific/Apia",
}

# Fractional-hour offsets in total minutes → TZ name
_FRAC_OFFSET_TO_TZ: dict[int, str] = {
    -210: "America/St_Johns",  # −3:30
    330: "Asia/Kolkata",        # +5:30
    345: "Asia/Kathmandu",      # +5:45
    390: "Asia/Yangon",         # +6:30
    570: "Australia/Darwin",    # +9:30
    630: "Australia/Lord_Howe", # +10:30
}


# ---------------------------------------------------------------------------
# Timezone + time parsing helpers
# ---------------------------------------------------------------------------

def _parse_time_str(text: str) -> str:
    """
    Extracts the first time expression from text and returns "HH:MM".
    Falls back to "08:00" if nothing found.
    """
    # 12-hour with am/pm: "7:30am", "8 pm", "9:00 AM"
    m = re.search(r'\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b', text.lower())
    if m:
        h, mins, ampm = int(m.group(1)), int(m.group(2) or 0), m.group(3)
        if ampm == "pm" and h < 12:
            h += 12
        elif ampm == "am" and h == 12:
            h = 0
        if 0 <= h <= 23 and 0 <= mins <= 59:
            return f"{h:02d}:{mins:02d}"
    # 24-hour with colon: "07:30", "14:00"
    m = re.search(r'\b(\d{1,2}):(\d{2})\b', text)
    if m:
        h, mins = int(m.group(1)), int(m.group(2))
        if 0 <= h <= 23 and 0 <= mins <= 59:
            return f"{h:02d}:{mins:02d}"
    return "08:00"


def parse_timezone_and_time(text: str) -> tuple[str, str]:
    """
    Parses text like "New York, 7:30am" or "EST" or "UTC+8" or "London".
    Returns (pytz_tz_string, "HH:MM").  Falls back to ("UTC", "08:00").
    """
    time_str = _parse_time_str(text)
    lower = text.lower().strip()

    # 1. UTC/GMT offset: "+5:30", "UTC+8", "-5", "GMT-4:30"
    offset_match = re.search(
        r'(?:utc|gmt)?\s*([+\-])(\d{1,2})(?::(\d{2}))?(?!\d)',
        lower,
    )
    if offset_match:
        sign = 1 if offset_match.group(1) == "+" else -1
        oh = int(offset_match.group(2))
        om = int(offset_match.group(3)) if offset_match.group(3) else 0
        total_min = sign * (oh * 60 + om)
        tz_name = _FRAC_OFFSET_TO_TZ.get(total_min)
        if not tz_name and om == 0:
            tz_name = _HOUR_OFFSET_TO_TZ.get(sign * oh)
        return (tz_name or "UTC"), time_str

    # 2. Abbreviation (whole-word match)
    for abbr, tz_name in TZ_ABBR_MAP.items():
        if re.search(r'\b' + re.escape(abbr) + r'\b', lower):
            return tz_name, time_str

    # 3. City/region name (longest match first to avoid partial matches)
    for city in sorted(CITY_TO_TZ.keys(), key=len, reverse=True):
        if city in lower:
            return CITY_TO_TZ[city], time_str

    # 4. Direct IANA string (e.g. "America/Chicago")
    for word in text.split():
        try:
            pytz.timezone(word)
            return word, time_str
        except pytz.UnknownTimeZoneError:
            pass

    return "UTC", time_str


def _fmt_time_display(time_str: str) -> str:
    """Converts "HH:MM" to "7:30am" style."""
    h, m = [int(x) for x in time_str.split(":")]
    ampm = "am" if h < 12 else "pm"
    h12 = h % 12 or 12
    return f"{h12}:{m:02d}{ampm}"


# ---------------------------------------------------------------------------
# User data helpers
# ---------------------------------------------------------------------------

def load_users() -> dict:
    if not os.path.exists(USERS_FILE):
        return {}
    with open(USERS_FILE) as f:
        return json.load(f)


def save_users(users: dict) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(USERS_FILE, "w") as f:
        json.dump(users, f, indent=2)


def get_user(uid: str) -> dict | None:
    return load_users().get(uid)


def update_user(uid: str, fields: dict) -> None:
    users = load_users()
    if uid not in users:
        users[uid] = {}
    users[uid].update(fields)
    save_users(users)


# ---------------------------------------------------------------------------
# APScheduler — in-process briefing scheduler
# ---------------------------------------------------------------------------

def _should_send_briefing(user: dict, now_utc: datetime) -> bool:
    """
    Returns True if the user should receive a briefing at this exact minute.
    Called every minute by APScheduler — uses exact HH:MM match (no window).
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

    today_str = now_local.date().isoformat()
    if user.get("last_briefing_date") == today_str:
        return False

    briefing_time_str = user.get("briefing_time", "08:00")
    try:
        bh, bm = [int(x) for x in briefing_time_str.split(":")]
    except (ValueError, AttributeError):
        return False

    return now_local.hour == bh and now_local.minute == bm


def _scheduler_tick() -> None:
    """Called every minute by APScheduler. Sends briefings to eligible users."""
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    if not bot_token:
        return

    now_utc = datetime.now(pytz.utc)
    users = load_users()
    eligible = [u for u in users.values() if _should_send_briefing(u, now_utc)]

    if not eligible:
        return

    logger.info(f"APScheduler: {len(eligible)} user(s) eligible — sending briefing(s).")

    async def _run():
        tasks = [generate_briefing_for_user(u, bot_token) for u in eligible]
        await asyncio.gather(*tasks, return_exceptions=True)

    asyncio.run(_run())


def _build_eod_cache_tick() -> None:
    """Called at 4:30pm ET Mon-Fri by APScheduler. Builds the EOD close price cache."""
    from utils.eod_cache import build_eod_cache
    from utils.market_data import _get_client

    users = load_users()
    # Collect all unique watchlist entries (deduplicated by ticker)
    seen: set[str] = set()
    all_entries: list[dict] = []
    for u in users.values():
        for w in u.get("watchlist", []):
            t = w["ticker"]
            if t not in seen:
                seen.add(t)
                all_entries.append(w)
    try:
        build_eod_cache(all_entries, _get_client())
    except Exception as e:
        logger.error(f"EOD cache build failed: {e}")


def _start_apscheduler() -> None:
    """Starts APScheduler in a background thread."""
    scheduler = BackgroundScheduler()
    scheduler.add_job(_scheduler_tick, "interval", minutes=1, id="briefing_scheduler")
    scheduler.add_job(
        _build_eod_cache_tick,
        CronTrigger(
            day_of_week="mon-fri",
            hour=16,
            minute=30,
            timezone="America/New_York",
        ),
        id="eod_cache_builder",
    )
    scheduler.start()
    logger.info("APScheduler started — briefing tick every minute, EOD cache at 4:30pm ET.")


# ---------------------------------------------------------------------------
# Goals summarization via Claude
# ---------------------------------------------------------------------------

async def _summarize_goals(goals_raw: str, knowledge_level: str) -> str:
    """
    Calls Claude to produce a 2-3 sentence goals summary for the system prompt.
    Falls back to a truncated raw string if the API key is absent or the call fails.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        logger.warning("ANTHROPIC_API_KEY not set — using raw goals as summary fallback")
        return goals_raw[:500]

    try:
        client = anthropic.AsyncAnthropic(api_key=api_key)
        response = await client.messages.create(
            model=MODEL,
            max_tokens=200,
            messages=[{
                "role": "user",
                "content": (
                    "A user is setting up their personalized daily market briefing. "
                    f"Their knowledge level is: {knowledge_level}.\n\n"
                    f"They described their investing goals as:\n\"{goals_raw}\"\n\n"
                    "Write a 2-3 sentence summary of their goals and investing intent "
                    "written in the third person, to be used as context for an AI writing "
                    "their daily market briefings. Be specific and concrete. "
                    "Start with 'User is...' or 'User wants...'"
                ),
            }],
        )
        return response.content[0].text.strip()
    except Exception as e:
        logger.error(f"Goals summarization failed: {e}")
        return goals_raw[:500]


# ---------------------------------------------------------------------------
# Watchlist parsing helpers
# ---------------------------------------------------------------------------

_SHEETS_URL_RE = re.compile(r"https://docs\.google\.com/spreadsheets/")


async def _process_tickers(
    tickers: list[str],
    update: Update,
) -> tuple[list[dict], list[str]] | None:
    """
    Validates a list of raw tickers. Sends progress + result messages.
    Returns (valid, invalid) or None on hard failure.
    """
    if not tickers:
        await update.message.reply_text(
            "I couldn't find any tickers. Please enter them like: VOO, NVDA, AAPL"
        )
        return None

    await update.message.reply_text(f"Validating {len(tickers)} ticker(s)...")
    valid, invalid = await validate_tickers_parallel(tickers)

    if not valid:
        await update.message.reply_text(
            f"None of the tickers could be validated: {', '.join(invalid)}\n"
            "Please check your spelling and try again."
        )
        return None

    return valid, invalid


# ---------------------------------------------------------------------------
# /start — entry point
# ---------------------------------------------------------------------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    uid = str(user.id)
    existing = get_user(uid)

    if existing and existing.get("onboarding_complete"):
        await update.message.reply_text(
            f"Welcome back, {user.first_name}! 👋\n\n"
            "Type *Help* to see all available commands.",
            parse_mode="Markdown",
        )
        return ConversationHandler.END

    if not existing:
        update_user(uid, {
            "telegram_id": user.id,
            "first_name": user.first_name,
            "onboarding_complete": False,
            "watchlist": [],
        })
        logger.info(f"New user started onboarding: {user.id} ({user.first_name})")

    await update.message.reply_text(
        "Welcome\\! I'm your daily Market Brief bot 📊\n\n"
        "I'll send you a personalized market briefing every weekday morning "
        "to help you learn investing and stay on top of your portfolio\\.\n\n"
        "Let's get you set up — it takes less than 2 minutes\\.\n\n"
        "First, send me your watchlist\\. You can:\n"
        "• Type tickers separated by commas \\(e\\.g\\. VOO, NVDA, AAPL\\)\n"
        "• Share a Google Sheets link\n"
        "• Upload an Excel file \\(\\.xlsx\\)\n\n"
        "📌 *Crypto assets must include the \\-USD suffix* \\(e\\.g\\. BTC\\-USD, ETH\\-USD, SOL\\-USD\\)\\.\n\n"
        "You can always add or remove securities later by messaging me anytime\\.",
        parse_mode="MarkdownV2",
    )
    return WATCHLIST


# ---------------------------------------------------------------------------
# Onboarding Step 1 — Watchlist
# ---------------------------------------------------------------------------

async def handle_watchlist(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    uid = str(update.effective_user.id)
    text = update.message.text.strip()

    # Google Sheets URL
    if _SHEETS_URL_RE.search(text):
        await update.message.reply_text("Fetching your watchlist from Google Sheets...")
        try:
            raw_tickers = await asyncio.to_thread(fetch_tickers_from_sheets, text)
        except ValueError as e:
            await update.message.reply_text(f"Could not read the sheet: {e}\nPlease try again.")
            return WATCHLIST
    else:
        # Comma or whitespace separated tickers
        raw_tickers = [
            t.strip().upper()
            for t in re.split(r"[,\s]+", text)
            if t.strip() and re.match(r"^[A-Z0-9.\-\^]+$", t.strip().upper())
        ]

    result = await _process_tickers(raw_tickers, update)
    if result is None:
        return WATCHLIST

    valid, invalid = result
    update_user(uid, {"watchlist": valid})

    resp = f"Added {len(valid)} ticker(s) to your watchlist: {', '.join(w['ticker'] for w in valid)} ✅"
    if invalid:
        resp += f"\n\nCould not validate and skipped: {', '.join(invalid)}"
    await update.message.reply_text(resp)

    return await _ask_knowledge_level(update)


async def handle_watchlist_document(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    """Handles .xlsx file uploads during the watchlist step."""
    uid = str(update.effective_user.id)
    doc = update.message.document

    if not (doc.file_name or "").lower().endswith(".xlsx"):
        await update.message.reply_text(
            "Please upload an *.xlsx* file\\.",
            parse_mode="MarkdownV2",
        )
        return WATCHLIST

    await update.message.reply_text("Downloading file...")
    tg_file = await doc.get_file()

    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        await tg_file.download_to_drive(tmp_path)
        raw_tickers = await asyncio.to_thread(parse_excel_tickers, tmp_path)
    except Exception as e:
        logger.error(f"Excel download/parse error: {e}")
        await update.message.reply_text(
            "Couldn't read the file. Please make sure it's a valid .xlsx and try again."
        )
        return WATCHLIST
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    result = await _process_tickers(raw_tickers, update)
    if result is None:
        return WATCHLIST

    valid, invalid = result
    update_user(uid, {"watchlist": valid})

    resp = f"Added {len(valid)} ticker(s): {', '.join(w['ticker'] for w in valid)} ✅"
    if invalid:
        resp += f"\n\nSkipped (could not validate): {', '.join(invalid)}"
    await update.message.reply_text(resp)

    return await _ask_knowledge_level(update)


async def _ask_knowledge_level(update: Update) -> int:
    await update.effective_message.reply_text(
        "How familiar are you with investing\\? This helps me calibrate "
        "how I explain things in your daily briefing — the goal is to "
        "keep teaching you new concepts at the right pace\\.\n\n"
        "Reply with a number:\n"
        "1 — *Beginner*: Just starting out\\. Explain everything simply "
        "and teach me the jargon as we go\\.\n"
        "2 — *Intermediate*: I know the basics — stocks, ETFs, sectors, P/E ratios\\.\n"
        "3 — *Advanced*: I'm comfortable with financial concepts\\. Keep "
        "it dense and don't over\\-explain\\.\n\n"
        "You can change this anytime: 'change my knowledge level'",
        parse_mode="MarkdownV2",
    )
    return KNOWLEDGE_LEVEL


# ---------------------------------------------------------------------------
# Onboarding Step 2 — Knowledge Level
# ---------------------------------------------------------------------------

KNOWLEDGE_MAP = {"1": "beginner", "2": "intermediate", "3": "advanced"}


async def handle_knowledge_level(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    uid = str(update.effective_user.id)
    text = update.message.text.strip()

    if text not in KNOWLEDGE_MAP:
        await update.message.reply_text(
            "Please reply with *1*, *2*, or *3*\\.",
            parse_mode="MarkdownV2",
        )
        return KNOWLEDGE_LEVEL

    level = KNOWLEDGE_MAP[text]
    update_user(uid, {"knowledge_level": level})

    # Settings-change re-run: confirm and exit
    if context.user_data.pop("re_running", False):
        await update.message.reply_text(
            f"✅ Knowledge level updated to *{level.capitalize()}*\\.",
            parse_mode="MarkdownV2",
        )
        return ConversationHandler.END

    await update.message.reply_text(
        f"Got it — *{level.capitalize()}* it is\\. ✅\n\n"
        "How often do you want the 'Today's Concept' section — a short "
        "lesson tied to something that actually happened in the market that day\\?\n\n"
        "Reply with a number:\n"
        "1 — Every day\n"
        "2 — Three times a week \\(Monday, Wednesday, Friday\\)\n"
        "3 — Once a week \\(include your preferred day, e\\.g\\. '3, Friday'\\)\n"
        "4 — Never \\(you can always turn it on later\\)\n\n"
        "You can change this anytime: 'change concept frequency'",
        parse_mode="MarkdownV2",
    )
    return CONCEPT_FREQ


async def _ask_timezone(update: Update) -> int:
    await update.effective_message.reply_text(
        "Got it\\. ✅\n\n"
        "What's your timezone and what time would you like your morning "
        "briefing\\? \\(8am is the default\\)\n\n"
        "e\\.g\\. *New York*, *EST*, *London, 7:30am*, *UTC\\+8*",
        parse_mode="MarkdownV2",
    )
    return TIMEZONE_TIME


# ---------------------------------------------------------------------------
# Onboarding Step 3 — Concept Frequency
# ---------------------------------------------------------------------------

async def handle_concept_freq(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    uid = str(update.effective_user.id)
    text = update.message.text.strip()

    # Continuation: user is providing the day for option "3"
    if context.user_data.get("awaiting_concept_day"):
        day = text.strip().capitalize()
        freq = f"weekly:{day}"
        context.user_data.pop("awaiting_concept_day", None)
        update_user(uid, {"concept_frequency": freq})
        if context.user_data.pop("re_running", False):
            await update.message.reply_text(
                f"✅ Concept section updated to every *{day}*\\.",
                parse_mode="MarkdownV2",
            )
            return ConversationHandler.END
        return await _ask_timezone(update)

    freq = None
    lower = text.lower()
    if lower == "1":
        freq = "daily"
    elif lower == "2":
        freq = "mwf"
    elif lower == "4":
        freq = "off"
    elif lower.startswith("3"):
        parts = lower.split(",", 1)
        if len(parts) == 2:
            day = parts[1].strip().capitalize()
            freq = f"weekly:{day}"
        else:
            context.user_data["awaiting_concept_day"] = True
            await update.message.reply_text(
                "Which day would you like it\\? \\(e\\.g\\. Monday, Friday\\)",
                parse_mode="MarkdownV2",
            )
            return CONCEPT_FREQ

    if freq is None:
        await update.message.reply_text(
            "Please reply with *1*, *2*, *3*, or *4*\\.",
            parse_mode="MarkdownV2",
        )
        return CONCEPT_FREQ

    update_user(uid, {"concept_frequency": freq})

    # Settings-change re-run: confirm and exit
    if context.user_data.pop("re_running", False):
        freq_display = {
            "daily": "every day",
            "mwf": "Monday, Wednesday, Friday",
            "off": "never",
        }.get(freq, freq.replace("weekly:", "every "))
        await update.message.reply_text(f"✅ Concept section updated to {freq_display}.")
        return ConversationHandler.END

    return await _ask_timezone(update)


# ---------------------------------------------------------------------------
# Onboarding Step 4 — Timezone & Time
# ---------------------------------------------------------------------------

async def handle_timezone(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    uid = str(update.effective_user.id)
    text = update.message.text.strip()

    tz_str, time_str = parse_timezone_and_time(text)

    # Validate — fall back to UTC if unknown
    try:
        tz = pytz.timezone(tz_str)
    except pytz.UnknownTimeZoneError:
        tz_str = "UTC"
        tz = pytz.utc

    update_user(uid, {"timezone": tz_str, "briefing_time": time_str})

    time_display = _fmt_time_display(time_str)
    now = datetime.now(tz)
    utc_offset = now.strftime("%z")  # e.g. "+0530" or "-0500"
    if len(utc_offset) == 5:
        utc_display = f"UTC{utc_offset[:3]}:{utc_offset[3:]}"
    else:
        utc_display = "UTC"

    confirm_text = (
        f"Got it — *{_esc(time_display)}* {_esc(tz_str)} \\({_esc(utc_display)}\\) ✅\n\n"
    )

    # Settings-change re-run: confirm and exit
    if context.user_data.pop("re_running", False):
        await update.message.reply_text(
            confirm_text.rstrip("\n"),
            parse_mode="MarkdownV2",
        )
        return ConversationHandler.END

    await update.message.reply_text(
        confirm_text +
        "Almost done — last one\\.\n\n"
        "Tell me a little about yourself as an investor\\. Why are you "
        "here, and what are you hoping to get out of this daily briefing\\? "
        "Even 2\\-3 sentences is enough\\.\n\n"
        "There's no wrong answer — this just helps me make your briefing "
        "feel like it's written specifically for you\\.",
        parse_mode="MarkdownV2",
    )
    return GOALS


# ---------------------------------------------------------------------------
# Onboarding Step 5 — Goals
# ---------------------------------------------------------------------------

async def handle_goals(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    uid = str(update.effective_user.id)
    text = update.message.text.strip()

    user_data = get_user(uid) or {}
    knowledge_level = user_data.get("knowledge_level", "intermediate")

    update_user(uid, {"goals_raw": text})
    context.user_data["goals_raw"] = text

    await update.message.reply_text("Got it — give me a moment to personalize your setup...")

    summary = await _summarize_goals(text, knowledge_level)
    update_user(uid, {"goals_summary": summary})
    context.user_data["goals_summary"] = summary

    await update.message.reply_text(
        "Here's how I'll personalize your briefing based on what you've told me:\n\n"
        f"_{_esc(summary)}_\n\n"
        "Reply *'looks good'* to confirm, or correct anything I got wrong\\.",
        parse_mode="MarkdownV2",
    )
    return GOALS_CONFIRM


# ---------------------------------------------------------------------------
# Onboarding Step 6 — Goals Confirmation
# ---------------------------------------------------------------------------

_AFFIRMATIVES = frozenset({
    "looks good", "yes", "yep", "correct", "good", "perfect",
    "ok", "okay", "great", "confirmed", "sounds good", "lgtm",
})


async def handle_goals_confirm(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    uid = str(update.effective_user.id)
    text = update.message.text.strip().lower()

    if any(a in text for a in _AFFIRMATIVES):
        # Settings-change re-run: just update and exit
        if context.user_data.pop("re_running", False):
            await update.message.reply_text("✅ Goals summary updated\\.", parse_mode="MarkdownV2")
            return ConversationHandler.END
        return await _complete_onboarding(update, uid)

    # User sent a correction — regenerate summary with correction appended
    existing_raw = get_user(uid).get("goals_raw", "")
    combined = f"{existing_raw}. Additional context: {update.message.text.strip()}"
    update_user(uid, {"goals_raw": combined})
    context.user_data["goals_raw"] = combined

    await update.message.reply_text("Updating your summary...")

    user_data = get_user(uid) or {}
    knowledge_level = user_data.get("knowledge_level", "intermediate")
    summary = await _summarize_goals(combined, knowledge_level)
    update_user(uid, {"goals_summary": summary})
    context.user_data["goals_summary"] = summary

    await update.message.reply_text(
        "Here's the updated version:\n\n"
        f"_{_esc(summary)}_\n\n"
        "Reply *'looks good'* to confirm\\.",
        parse_mode="MarkdownV2",
    )
    return GOALS_CONFIRM


async def _complete_onboarding(update: Update, uid: str) -> int:
    update_user(uid, {"onboarding_complete": True})
    logger.info(f"User {uid} completed onboarding.")

    await update.message.reply_text(
        "You're all set 🎉\n\n"
        "Your first briefing arrives tomorrow at your set time\\.\n"
        "Here's what to expect each morning:\n\n"
        "🌍 *Market Pulse* — what happened in the market and why\n"
        "📂 *Your Sectors* — how your holdings moved as a group\n"
        "⚡ *Outlier Alert* — if any of your stocks made a big unusual move\n"
        "🎓 *Today's Concept* — one market idea taught through today's events\n"
        "📅 *On The Radar* — what's coming up relevant to your watchlist\n"
        "💡 *Security to Watch* — a new idea tailored to you \\(Fridays\\)\n\n"
        "To make changes anytime, just message me:\n"
        "• *Add \\[TICKER\\]* or *Remove \\[TICKER\\]* — manage your watchlist\n"
        "• *Show my watchlist* — see current tickers\n"
        "• *Change my knowledge level*\n"
        "• *Change concept frequency*\n"
        "• *Change my timezone or time*\n"
        "• *Update my goals*\n"
        "• *Help* — full list",
        parse_mode="MarkdownV2",
    )
    return ConversationHandler.END


# ---------------------------------------------------------------------------
# /watchlist — show watchlist with add/remove instructions
# ---------------------------------------------------------------------------

async def cmd_watchlist(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = str(update.effective_user.id)
    user = get_user(uid)
    if not user or not user.get("onboarding_complete"):
        await update.message.reply_text("Type /start to set up your account first.")
        return

    watchlist = user.get("watchlist", [])
    if watchlist:
        lines = [
            f"• {_esc(w['ticker'])} — {_esc(w.get('company_name', '—'))} \\({_esc(w.get('sector', 'Unknown'))}\\)"
            for w in watchlist
        ]
        text = (
            "*Your watchlist:*\n" + "\n".join(lines) +
            "\n\nTo add: send *Add TICKER* \\(e\\.g\\. Add TSLA\\)\n"
            "To remove: send *Remove TICKER* \\(e\\.g\\. Remove TSLA\\)"
        )
    else:
        text = (
            "Your watchlist is empty\\.\n\n"
            "To add tickers, send: *Add VOO, AAPL*"
        )
    await update.message.reply_text(text, parse_mode="MarkdownV2")


# ---------------------------------------------------------------------------
# /settings — inline keyboard menu
# ---------------------------------------------------------------------------

async def cmd_settings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = str(update.effective_user.id)
    user = get_user(uid)
    if not user or not user.get("onboarding_complete"):
        await update.message.reply_text("Type /start to set up your account first.")
        return

    keyboard = [
        [
            InlineKeyboardButton("📊 Knowledge Level", callback_data="settings:knowledge"),
            InlineKeyboardButton("💡 Concept Frequency", callback_data="settings:concepts"),
        ],
        [
            InlineKeyboardButton("🕐 Timezone & Time", callback_data="settings:timezone"),
            InlineKeyboardButton("🎯 Investment Goals", callback_data="settings:goals"),
        ],
        [
            InlineKeyboardButton("📈 View / Manage Watchlist", callback_data="settings:watchlist"),
        ],
    ]
    await update.message.reply_text(
        "What would you like to update?",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def handle_settings_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    await query.answer()

    uid = str(update.effective_user.id)
    user = get_user(uid)
    if not user or not user.get("onboarding_complete"):
        await query.message.reply_text("Type /start to set up your account first.")
        return ConversationHandler.END

    context.user_data["re_running"] = True
    data = query.data

    if data == "settings:knowledge":
        return await _ask_knowledge_level(update)

    if data == "settings:concepts":
        await query.message.reply_text(
            "How often would you like the 'Today's Concept' section\\?\n\n"
            "1 — Every day\n"
            "2 — Three times a week \\(Mon, Wed, Fri\\)\n"
            "3 — Once a week \\(include your preferred day, e\\.g\\. '3, Friday'\\)\n"
            "4 — Never",
            parse_mode="MarkdownV2",
        )
        return CONCEPT_FREQ

    if data == "settings:timezone":
        return await _ask_timezone(update)

    if data == "settings:goals":
        await query.message.reply_text(
            "Tell me about your investing goals and what you want from your daily briefing\\. "
            "Even 2\\-3 sentences is enough\\.",
            parse_mode="MarkdownV2",
        )
        return GOALS

    if data == "settings:watchlist":
        context.user_data.pop("re_running", None)  # not entering a flow
        watchlist = user.get("watchlist", [])
        if watchlist:
            lines = [
                f"• {w['ticker']} — {w.get('company_name', '—')} \\({w.get('sector', 'Unknown')}\\)"
                for w in watchlist
            ]
            text = (
                "*Your watchlist:*\n" + "\n".join(lines) +
                "\n\nTo add: send *Add TICKER* \\(e\\.g\\. Add TSLA\\)\n"
                "To remove: send *Remove TICKER* \\(e\\.g\\. Remove TSLA\\)"
            )
        else:
            text = (
                "Your watchlist is empty\\.\n\n"
                "To add tickers, send: *Add VOO, AAPL*"
            )
        await query.message.reply_text(text, parse_mode="MarkdownV2")
        return ConversationHandler.END

    return ConversationHandler.END


# ---------------------------------------------------------------------------
# Re-entry handlers for settings changes
# ---------------------------------------------------------------------------

async def re_knowledge_level(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    uid = str(update.effective_user.id)
    if not _is_onboarded(uid, update):
        return ConversationHandler.END
    context.user_data["re_running"] = True
    return await _ask_knowledge_level(update)


async def re_concept_freq(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    uid = str(update.effective_user.id)
    if not _is_onboarded(uid, update):
        return ConversationHandler.END
    context.user_data["re_running"] = True
    await update.message.reply_text(
        "How often would you like the 'Today's Concept' section\\?\n\n"
        "1 — Every day\n"
        "2 — Three times a week \\(Mon, Wed, Fri\\)\n"
        "3 — Once a week \\(include your preferred day, e\\.g\\. '3, Friday'\\)\n"
        "4 — Never",
        parse_mode="MarkdownV2",
    )
    return CONCEPT_FREQ


async def re_timezone(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    uid = str(update.effective_user.id)
    if not _is_onboarded(uid, update):
        return ConversationHandler.END
    context.user_data["re_running"] = True
    return await _ask_timezone(update)


async def re_goals(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    uid = str(update.effective_user.id)
    if not _is_onboarded(uid, update):
        return ConversationHandler.END
    context.user_data["re_running"] = True
    await update.message.reply_text(
        "Tell me about your investing goals and what you want from your daily briefing\\. "
        "Even 2\\-3 sentences is enough\\.",
        parse_mode="MarkdownV2",
    )
    return GOALS


def _is_onboarded(uid: str, update: Update) -> bool:
    """Checks onboarding status; sends a nudge message if not complete."""
    user_data = get_user(uid)
    if not user_data or not user_data.get("onboarding_complete"):
        return False
    return True


# ---------------------------------------------------------------------------
# Ongoing commands (existing users)
# ---------------------------------------------------------------------------

async def cmd_brief(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Generate and send the full briefing immediately — for testing."""
    from briefing import generate_briefing_for_user

    uid = str(update.effective_user.id)
    user = get_user(uid)
    if not user or not user.get("onboarding_complete"):
        await update.message.reply_text("Type /start to set up your account first.")
        return
    if not user.get("watchlist"):
        await update.message.reply_text(
            "Your watchlist is empty — add some tickers first \\(e\\.g\\. *Add VOO, AAPL*\\)\\.",
            parse_mode="MarkdownV2",
        )
        return

    await update.message.reply_text("Generating your briefing… this may take 20–30 seconds ⏳")

    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    try:
        await generate_briefing_for_user(user, token)
    except Exception as e:
        logger.error(f"cmd_brief error for {uid}: {e}")
        await update.message.reply_text(
            "Something went wrong generating your briefing\\. Check the logs\\.",
            parse_mode="MarkdownV2",
        )


async def cmd_debug(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Fetch and display raw Finnhub quote data for watchlist + SPY/QQQ/DIA."""
    uid = str(update.effective_user.id)
    user = get_user(uid)
    if not user or not user.get("onboarding_complete"):
        await update.message.reply_text("Type /start to set up your account first.")
        return

    watchlist = user.get("watchlist", [])
    tickers = sorted({w["ticker"] for w in watchlist} | {"SPY", "QQQ", "DIA"})

    await update.message.reply_text("Fetching raw Finnhub quotes...")

    def _fetch_quotes() -> list[tuple]:
        client = _get_client()
        rows = []
        for symbol in tickers:
            try:
                q = client.quote(symbol)
                rows.append((symbol, q))
            except Exception as e:
                rows.append((symbol, f"ERROR: {e}"))
        return rows

    rows = await asyncio.to_thread(_fetch_quotes)

    from datetime import timezone as _tz
    ts = datetime.now(_tz.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    lines = [f"[FINNHUB DEBUG - {ts}]", ""]

    for symbol, q in rows:
        if isinstance(q, str):
            # Exception string
            lines.append(f"{symbol:<6}  {q}")
        elif not q or q.get("c", 0) == 0:
            lines.append(f"{symbol:<6}  NO DATA (c=0)")
        else:
            lines.append(
                f"{symbol:<6}  c={q.get('c')}  dp={q.get('dp')}%"
                f"  o={q.get('o')}  pc={q.get('pc')}  d={q.get('d')}"
            )

    await update.message.reply_text("\n".join(lines))


async def cmd_migrate(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """One-time migration: resolve bare tickers to yf_symbol/asset_type/finnhub_symbol."""
    from utils.ticker_resolver import resolve_ticker

    await update.message.reply_text("Running ticker migration — please wait...")

    def _run_migration() -> tuple[list[str], int, int]:
        users = load_users()
        lines: list[str] = []
        total_resolved = 0
        users_affected = 0
        client = _get_client()

        for uid, profile in users.items():
            watchlist = profile.get("watchlist", [])
            user_resolved = 0
            for entry in watchlist:
                if "yf_symbol" not in entry:
                    bare = entry.get("ticker", "")
                    resolved = resolve_ticker(bare, client)
                    entry["yf_symbol"]      = resolved["yf_symbol"]
                    entry["asset_type"]     = resolved["asset_type"]
                    entry["finnhub_symbol"] = resolved["finnhub_symbol"]
                    lines.append(
                        f"[MIGRATED] {bare} → {resolved['yf_symbol']} ({resolved['asset_type']})"
                    )
                    user_resolved += 1
                    total_resolved += 1
            if user_resolved:
                users_affected += 1

        save_users(users)
        return lines, total_resolved, users_affected

    lines, total_resolved, users_affected = await asyncio.to_thread(_run_migration)

    summary_lines = lines if lines else ["No tickers needed migration."]
    summary_lines.append(
        f"\nMigration complete: {total_resolved} tickers resolved for {users_affected} users"
    )

    # Telegram message limit is 4096 chars; chunk if needed
    message = "\n".join(summary_lines)
    for i in range(0, len(message), 4000):
        await update.message.reply_text(message[i : i + 4000])


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "*Available commands:*\n\n"
        "/brief — generate your briefing right now\n"
        "/settings — update your preferences\n"
        "/watchlist — view & manage your watchlist\n"
        "/knowledge — change knowledge level\n"
        "/concepts — change concept frequency\n"
        "/timezone — update timezone & briefing time\n"
        "/goals — update investment goals\n"
        "/help — show this list\n\n"
        "You can also send *Add TICKER* or *Remove TICKER* anytime\\.",
        parse_mode="MarkdownV2",
    )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Routes plain-text messages from fully-onboarded users:
      - Add [TICKER(S)]
      - Remove [TICKER]
      - Show my watchlist
      - Help
    """
    uid = str(update.effective_user.id)
    user = get_user(uid)
    raw_text = update.message.text.strip() if update.message.text else ""
    lower = raw_text.lower()

    # Not onboarded yet
    if not user or not user.get("onboarding_complete"):
        await update.message.reply_text(
            "Type /start to set up your account first\\.",
            parse_mode="MarkdownV2",
        )
        return

    # ── help ────────────────────────────────────────────────────────────
    if lower in ("help", "/help"):
        await cmd_help(update, context)
        return

    # ── show my watchlist ────────────────────────────────────────────────
    if "show my watchlist" in lower or lower == "watchlist":
        watchlist = user.get("watchlist", [])
        if not watchlist:
            await update.message.reply_text("Your watchlist is currently empty.")
        else:
            lines = [
                f"• {w['ticker']} — {w.get('company_name', '—')} ({w.get('sector', 'Unknown')})"
                for w in watchlist
            ]
            await update.message.reply_text(
                "*Your watchlist:*\n" + "\n".join(lines),
                parse_mode="Markdown",
            )
        return

    # ── add [TICKER(S)] ──────────────────────────────────────────────────
    add_match = re.match(r'^add\s+(.+)$', lower)
    if add_match:
        ticker_str = raw_text[4:].strip()
        raw_tickers = [
            t.strip().upper()
            for t in re.split(r"[,\s]+", ticker_str)
            if t.strip()
        ]

        result = await _process_tickers(raw_tickers, update)
        if result is None:
            return

        valid, invalid = result
        current_watchlist = user.get("watchlist", [])
        existing_tickers = {w["ticker"] for w in current_watchlist}

        added = [w for w in valid if w["ticker"] not in existing_tickers]
        already_there = [w["ticker"] for w in valid if w["ticker"] in existing_tickers]

        updated_watchlist = current_watchlist + added
        update_user(uid, {"watchlist": updated_watchlist})

        parts = []
        if added:
            added_strs = [
                f"{w.get('yf_symbol', w['ticker'])} ({w.get('asset_type', 'EQUITY')})"
                for w in added
            ]
            parts.append(f"Added: {', '.join(added_strs)} ✅")
        if already_there:
            parts.append(f"Already in watchlist: {', '.join(already_there)}")
        if invalid:
            parts.append(
                f"Could not find a security matching: {', '.join(invalid)}. "
                "Please check the symbol(s) and try again."
            )

        await update.message.reply_text("\n".join(parts) or "No changes made.")
        return

    # ── remove [TICKER] ──────────────────────────────────────────────────
    remove_match = re.match(r'^remove\s+(\S+)$', lower)
    if remove_match:
        ticker = raw_text[7:].strip().upper()
        current_watchlist = user.get("watchlist", [])
        new_watchlist = [w for w in current_watchlist if w["ticker"] != ticker]

        if len(new_watchlist) == len(current_watchlist):
            await update.message.reply_text(
                f"{ticker} is not in your watchlist."
            )
        else:
            update_user(uid, {"watchlist": new_watchlist})
            await update.message.reply_text(f"Removed {ticker} from your watchlist. ✅")
        return

    # ── unrecognized ─────────────────────────────────────────────────────
    await update.message.reply_text(
        "I didn't recognize that command\\. Type *Help* to see what I can do\\.",
        parse_mode="MarkdownV2",
    )


# ---------------------------------------------------------------------------
# Fallback
# ---------------------------------------------------------------------------

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.clear()
    await update.message.reply_text(
        "Setup cancelled\\. Type /start whenever you're ready to begin\\.",
        parse_mode="MarkdownV2",
    )
    return ConversationHandler.END


# ---------------------------------------------------------------------------
# Application setup
# ---------------------------------------------------------------------------

async def post_init(application: Application) -> None:
    """Register bot commands so they appear in Telegram's '/' menu."""
    await application.bot.set_my_commands([])  # clear any previously registered commands
    await application.bot.set_my_commands([
        BotCommand("start",     "Set up your account"),
        BotCommand("brief",     "Generate your briefing right now"),
        BotCommand("settings",  "Update your preferences"),
        BotCommand("watchlist", "View & manage your watchlist"),
        BotCommand("knowledge", "Change knowledge level"),
        BotCommand("concepts",  "Change concept frequency"),
        BotCommand("timezone",  "Update timezone & briefing time"),
        BotCommand("goals",     "Update investment goals"),
        BotCommand("help",      "Show available commands"),
        BotCommand("cancel",    "Cancel current operation"),
        # /debug intentionally excluded — still works when typed manually
    ])


_ASSET_CLASS_MAP: dict[str, str] = {
    "CRYPTOCURRENCY": "Crypto",
    "ETF":            "ETF",
    "EQUITY":         "Stock",
}


def _backfill_asset_class() -> None:
    """
    On startup, fills asset_class (and sub_category for stocks) for any
    watchlist entry where asset_class is absent or None.
    Uses yfinance fast_info.quote_type — no hardcoded mappings.
    """
    users = load_users()
    changed = False

    for uid, profile in users.items():
        for entry in profile.get("watchlist", []):
            if entry.get("asset_class") is not None:
                continue  # already set

            ticker = entry.get("yf_symbol") or entry.get("ticker", "")
            if not ticker:
                continue

            try:
                quote_type = yf.Ticker(ticker).fast_info.quote_type
                if not quote_type:
                    raise ValueError("empty quote_type")
                asset_class = _ASSET_CLASS_MAP.get(quote_type, "Other")
                entry["asset_class"] = asset_class

                if asset_class == "Stock":
                    try:
                        entry["sub_category"] = yf.Ticker(ticker).info.get("sector") or None
                    except Exception:
                        entry["sub_category"] = None
                else:
                    entry["sub_category"] = None

                changed = True
                logger.info(f"BACKFILL: {ticker} → {asset_class}")
            except Exception as e:
                bare = entry.get("ticker", ticker)
                logger.warning(
                    f"WARNING: {bare} could not be resolved (error: {e}). "
                    f"If this is a crypto asset, re-enter it as {bare}-USD "
                    f"via the Add command (e.g. 'Add {bare}-USD')."
                )

    if changed:
        save_users(users)
        logger.info("BACKFILL: asset_class backfill complete, users.json updated")


def main() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    _backfill_asset_class()

    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        raise ValueError("TELEGRAM_BOT_TOKEN is not set in environment / .env file")

    app = Application.builder().token(token).post_init(post_init).build()

    # ── Conversation handler (onboarding + settings re-runs) ─────────────
    onboarding = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            # Slash command re-entries (show up in Telegram's '/' menu)
            CommandHandler("knowledge", re_knowledge_level),
            CommandHandler("concepts",  re_concept_freq),
            CommandHandler("timezone",  re_timezone),
            CommandHandler("goals",     re_goals),
            # Inline keyboard callback re-entries
            CallbackQueryHandler(handle_settings_callback, pattern="^settings:"),
            # Legacy text-based re-entries
            MessageHandler(
                filters.TEXT & filters.Regex(r"(?i)change\s+my\s+knowledge\s+level"),
                re_knowledge_level,
            ),
            MessageHandler(
                filters.TEXT & filters.Regex(r"(?i)change\s+concept\s+(freq|frequency)"),
                re_concept_freq,
            ),
            MessageHandler(
                filters.TEXT & filters.Regex(r"(?i)change\s+my\s+(timezone|time)"),
                re_timezone,
            ),
            MessageHandler(
                filters.TEXT & filters.Regex(r"(?i)update\s+my\s+goals"),
                re_goals,
            ),
        ],
        states={
            WATCHLIST: [
                MessageHandler(
                    filters.Document.FileExtension("xlsx"),
                    handle_watchlist_document,
                ),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_watchlist),
            ],
            KNOWLEDGE_LEVEL: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_knowledge_level),
            ],
            CONCEPT_FREQ: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_concept_freq),
            ],
            TIMEZONE_TIME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_timezone),
            ],
            GOALS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_goals),
            ],
            GOALS_CONFIRM: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_goals_confirm),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
    )

    app.add_handler(onboarding)
    app.add_handler(CommandHandler("brief",     cmd_brief))
    app.add_handler(CommandHandler("debug",     cmd_debug))
    app.add_handler(CommandHandler("migrate",   cmd_migrate))  # hidden — not in menu
    app.add_handler(CommandHandler("help",      cmd_help))
    app.add_handler(CommandHandler("settings",  cmd_settings))
    app.add_handler(CommandHandler("watchlist", cmd_watchlist))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # Start health-check server for Render in a background daemon thread
    threading.Thread(target=_start_health_server, daemon=True).start()

    # Start in-process briefing scheduler (replaces GitHub Actions cron)
    _start_apscheduler()

    # --- Webhook swap point ---
    # To switch from polling to Telegram webhook mode (recommended for production on Render):
    #   1. Remove the _start_health_server() thread above (webhook runner handles its own HTTP server)
    #   2. Replace app.run_polling(...) below with:
    #      app.run_webhook(
    #          listen="0.0.0.0",
    #          port=int(os.environ.get("PORT", 8080)),
    #          webhook_url=os.environ["WEBHOOK_URL"],  # e.g. https://your-app.onrender.com/<TOKEN>
    #      )
    logger.info("MarketBrief bot is running...")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
