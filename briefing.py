"""
briefing.py
Core briefing pipeline.
Called by scheduler.py for each eligible user.
"""

import asyncio
import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path

import anthropic
import pytz
from telegram import Bot
from telegram.constants import ParseMode

from utils.market_data import (
    get_index_data,
    get_sector_data,
    get_stock_data,
    get_earnings_calendar,
    SECTOR_TO_ETF,
)
from utils.news import get_market_headlines, get_stock_news
from utils.calendar_utils import is_trading_day, get_session_dates
from utils.formatting import build_briefing_from_response, split_message
from prompts.system_prompt import build_system_prompt
from prompts.market_pulse import build_market_pulse_prompt
from prompts.sectors import build_sectors_prompt
from prompts.outlier import build_outlier_prompt
from prompts.concept import build_concept_prompt
from prompts.radar import build_radar_prompt
from prompts.security_watch import build_security_watch_prompt

logger = logging.getLogger(__name__)

MODEL = "claude-sonnet-4-6"

USERS_FILE = Path(__file__).parent / "data" / "users.json"

# Outlier thresholds: absolute |% move| required per volatility tier
OUTLIER_THRESHOLDS: dict[str, float] = {
    "low": 3.0,
    "medium": 5.0,
    "high": 8.0,
}

# Stock must also diverge from its sector ETF by at least this much to qualify
SECTOR_DIVERGENCE_MIN: float = 1.0


# ── Outlier detection ────────────────────────────────────────────────────────

def detect_outliers(
    stock_data: dict,
    sector_data: dict,
    watchlist: list[dict],
) -> list[dict]:
    """
    Returns list of outlier entries for watchlist stocks that:
      1. Moved more than their volatility-tier threshold (absolute %)
      2. Diverged from their sector ETF average by >= SECTOR_DIVERGENCE_MIN %
    """
    outliers = []
    for item in watchlist:
        ticker = item["ticker"]
        vol_tier = item.get("volatility_tier", "medium")
        sector = item.get("sector", "")

        sd = stock_data.get(ticker)
        if not sd:
            continue

        change_pct: float = sd["change_pct"]
        threshold = OUTLIER_THRESHOLDS.get(vol_tier, OUTLIER_THRESHOLDS["medium"])

        if abs(change_pct) < threshold:
            continue

        # Divergence check against sector ETF
        etf = SECTOR_TO_ETF.get(sector, "SPY")
        sector_d = sector_data.get(etf)
        sector_change: float = sector_d["change_pct"] if sector_d else 0.0

        if abs(change_pct - sector_change) < SECTOR_DIVERGENCE_MIN:
            continue

        outliers.append({
            "ticker": ticker,
            "change_pct": change_pct,
            "volatility_tier": vol_tier,
            "sector": sector,
            "sector_change_pct": sector_change,
        })

    return outliers


# ── Concept frequency gate ────────────────────────────────────────────────────

def should_show_concept(concept_frequency: str, dt: datetime) -> bool:
    """
    Returns True if the Today's Concept section should appear.

    concept_frequency values:
      "daily"          → always True
      "mwf"            → Monday, Wednesday, Friday
      "weekly:Monday"  → only that specific weekday
      "off"            → never
    """
    if concept_frequency == "off":
        return False
    if concept_frequency == "daily":
        return True
    if concept_frequency == "mwf":
        return dt.weekday() in (0, 2, 4)  # Mon=0, Wed=2, Fri=4
    if concept_frequency.startswith("weekly:"):
        day_name = concept_frequency.split(":", 1)[1].strip().capitalize()
        day_map = {
            "Monday": 0, "Tuesday": 1, "Wednesday": 2,
            "Thursday": 3, "Friday": 4, "Saturday": 5, "Sunday": 6,
        }
        target = day_map.get(day_name)
        return target is not None and dt.weekday() == target
    return False


# ── Persist briefing date ─────────────────────────────────────────────────────

def _save_last_briefing_date(telegram_id: str, date_str: str) -> None:
    """Writes last_briefing_date to users.json without importing main.py."""
    try:
        with open(USERS_FILE) as f:
            users = json.load(f)
        if telegram_id in users:
            users[telegram_id]["last_briefing_date"] = date_str
            with open(USERS_FILE, "w") as f:
                json.dump(users, f, indent=2)
    except Exception as e:
        logger.warning(f"Could not update last_briefing_date for {telegram_id}: {e}")


# ── Telegram send helpers ─────────────────────────────────────────────────────

def _sanitize_markdown(text: str) -> str:
    """
    Sanitize Claude's output for Telegram Markdown v1 (ParseMode.MARKDOWN).
    Fixes the most common parse errors without stripping intentional formatting:
    - Converts **double-asterisk bold** to *single-asterisk bold* (Telegram v1 syntax)
    - Escapes underscores within compound words (e.g. year_over_year) to prevent
      false italic/underline parsing
    - Escapes lone square brackets that aren't part of [link](url) syntax
    """
    # **bold** → *bold*  (Claude may emit CommonMark; Telegram v1 uses single *)
    text = re.sub(r'\*\*(.+?)\*\*', r'*\1*', text, flags=re.DOTALL)
    # Escape _ between word characters to prevent accidental italic parsing
    text = re.sub(r'(?<=\w)_(?=\w)', r'\\_', text)
    # Escape [ that isn't the start of a [text](url) inline link
    text = re.sub(r'\[(?![^\[\]\n]+\]\()', r'\\[', text)
    return text


async def _send_chunk_with_retry(
    bot: Bot,
    chat_id: int,
    text: str,
    max_retries: int = 3,
    retry_delay: float = 2.0,
) -> bool:
    """
    Send one message chunk with up to max_retries attempts.
    Returns True on success, False after all retries are exhausted.
    """
    for attempt in range(1, max_retries + 1):
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=ParseMode.MARKDOWN,
            )
            return True
        except Exception as e:
            if attempt < max_retries:
                logger.warning(
                    f"Chunk send failed (attempt {attempt}/{max_retries}): {e} "
                    f"— retrying in {retry_delay}s"
                )
                await asyncio.sleep(retry_delay)
            else:
                logger.error(f"Chunk send failed after {max_retries} attempts: {e}")
    return False


# ── Main pipeline ─────────────────────────────────────────────────────────────

async def generate_briefing_for_user(user: dict, bot_token: str) -> None:
    """
    Full briefing pipeline for a single user:

    1. Determine local datetime.
    2. Fetch market data, headlines, stock data, earnings in parallel.
    3. Fetch sector data for the user's sectors.
    4. Detect outliers; fetch news only for outlier tickers.
    5. Assemble section prompts (conditional sections skipped when not due).
    6. Single streaming Claude call.
    7. Format + split for Telegram 4096-char limit.
    8. Send via Telegram Bot.
    9. Persist last_briefing_date.
    """
    telegram_id = str(user["telegram_id"])
    watchlist: list[dict] = user.get("watchlist", [])
    tickers = [w["ticker"] for w in watchlist]

    # ── 1. Local datetime ────────────────────────────────────────────────
    tz_str = user.get("timezone", "UTC")
    try:
        tz = pytz.timezone(tz_str)
    except pytz.UnknownTimeZoneError:
        tz = pytz.utc

    now_local = datetime.now(tz)
    today_local = now_local.date()
    is_friday = now_local.weekday() == 4

    logger.info(
        f"Generating briefing for user {telegram_id} "
        f"({now_local.strftime('%Y-%m-%d %H:%M %Z')})"
    )

    # ── Trading day / session context ────────────────────────────────────
    import pytz as _pytz
    _et = _pytz.timezone("America/New_York")
    _now_et = datetime.now(_et)
    _et_minutes = _now_et.hour * 60 + _now_et.minute
    if _et_minutes < 17 * 60 + 30:
        brief_mode = "previous_session"
        session_label = "yesterday"
    else:
        brief_mode = "current_session"
        session_label = "today"

    market_open_today = is_trading_day("NYSE", today_local)

    # ── 2. Parallel data fetch ───────────────────────────────────────────
    base_tasks = [
        asyncio.to_thread(get_index_data),
        asyncio.to_thread(get_market_headlines, 5),
    ]
    if tickers:
        base_tasks.append(asyncio.to_thread(get_stock_data, tickers))
        base_tasks.append(asyncio.to_thread(get_earnings_calendar, tickers))

    results = await asyncio.gather(*base_tasks, return_exceptions=True)

    index_data = results[0] if not isinstance(results[0], Exception) else {}
    headlines = results[1] if not isinstance(results[1], Exception) else []
    stock_data = (results[2] if tickers and not isinstance(results[2], Exception) else {})
    earnings_calendar = (results[3] if tickers and not isinstance(results[3], Exception) else [])

    if isinstance(results[0], Exception):
        logger.error(f"get_index_data failed: {results[0]}")
    if isinstance(results[1], Exception):
        logger.error(f"get_market_headlines failed: {results[1]}")

    # ── 3. Sector data ───────────────────────────────────────────────────
    sectors_needed = list({w.get("sector", "") for w in watchlist if w.get("sector")})
    sector_data: dict = {}
    if sectors_needed:
        sector_data = await asyncio.to_thread(get_sector_data, sectors_needed)

    # ── 4. Outlier detection + targeted news ────────────────────────────
    outliers = detect_outliers(stock_data, sector_data, watchlist)

    outlier_news: dict = {}
    watchlist_news: dict = {}  # populated when market is closed

    if not market_open_today:
        # Market closed — fetch news for ALL watchlist tickers
        all_news_coros = {
            w["ticker"]: asyncio.to_thread(
                get_stock_news, w["ticker"], w.get("company_name", w["ticker"]), 3
            )
            for w in watchlist
        }
        all_news_results = await asyncio.gather(
            *all_news_coros.values(), return_exceptions=True
        )
        for ticker, result in zip(all_news_coros.keys(), all_news_results):
            watchlist_news[ticker] = (
                result if not isinstance(result, Exception) else []
            )
    elif outliers:
        news_coros = {}
        for o in outliers:
            t = o["ticker"]
            company = next(
                (w.get("company_name", t) for w in watchlist if w["ticker"] == t), t
            )
            news_coros[t] = asyncio.to_thread(get_stock_news, t, company, 3)

        news_results = await asyncio.gather(*news_coros.values(), return_exceptions=True)
        for ticker, result in zip(news_coros.keys(), news_results):
            outlier_news[ticker] = result if not isinstance(result, Exception) else []

    # ── 5. Assemble section prompts ──────────────────────────────────────
    section_prompts: list[str] = []

    # Session context header — tells Claude which session the % changes belong to
    _date_str = today_local.strftime("%A, %B %-d, %Y")
    _context_header = (
        f"TIME CONTEXT: Briefing generated at {now_local.strftime('%I:%M %p %Z')} "
        f"on {_date_str}. "
        f"Refer to all price changes as \"{session_label}'s performance\" throughout "
        f"the briefing unless stating a specific date."
    )
    if not market_open_today:
        _reason = "weekend" if today_local.weekday() >= 5 else "market holiday"
        _context_header += (
            f" NOTE: US equity markets are closed today ({_reason}). "
            f"Price data reflects the most recent completed trading session. "
            f"Focus on news and context rather than price movements."
        )
    section_prompts.append(_context_header)

    section_prompts.append(build_market_pulse_prompt(index_data, headlines))
    section_prompts.append(build_sectors_prompt(watchlist, sector_data, index_data, stock_data))

    if outliers:
        section_prompts.append(
            build_outlier_prompt(watchlist, stock_data, sector_data, outliers, outlier_news)
        )

    concept_freq = user.get("concept_frequency", "mwf")
    if should_show_concept(concept_freq, now_local):
        section_prompts.append(
            build_concept_prompt(index_data, sector_data, outliers, user)
        )

    section_prompts.append(build_radar_prompt(watchlist, earnings_calendar))

    if is_friday:
        section_prompts.append(build_security_watch_prompt(user))

    # When market is closed, append a watchlist news section for Claude to summarise
    if not market_open_today and watchlist_news:
        news_lines = ["SECTION: WATCHLIST NEWS (market closed)\n"]
        for ticker, items in watchlist_news.items():
            if items:
                news_lines.append(f"{ticker}:")
                for n in items:
                    news_lines.append(f"  - {n['title']} ({n['source']})")
            else:
                news_lines.append(f"{ticker}: No recent news.")
        news_lines.append(
            "\nWrite a brief 2-3 sentence summary of any notable news for "
            "the user's holdings while the market was closed. Skip tickers "
            "with no news. If nothing is notable, skip this section entirely."
        )
        section_prompts.append("\n".join(news_lines))

    # Join sections with a lightweight separator so Claude sees them as distinct tasks
    combined_prompt = "\n\n---\n\n".join(section_prompts)

    # ── 6. Single Claude streaming call ─────────────────────────────────
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    anthropic_client = anthropic.AsyncAnthropic(api_key=api_key)
    system_prompt = build_system_prompt(user)

    try:
        async with anthropic_client.messages.stream(
            model=MODEL,
            max_tokens=4096,
            system=system_prompt,
            messages=[{"role": "user", "content": combined_prompt}],
        ) as stream:
            final_message = await stream.get_final_message()

        claude_output: str = final_message.content[0].text
    except anthropic.APIConnectionError as e:
        logger.error(f"Claude connection error for {telegram_id}: {e}")
        return
    except anthropic.RateLimitError as e:
        logger.error(f"Claude rate limit for {telegram_id}: {e}")
        return
    except anthropic.APIStatusError as e:
        logger.error(f"Claude API status {e.status_code} for {telegram_id}: {e.message}")
        return
    except Exception as e:
        logger.error(f"Unexpected Claude error for {telegram_id}: {e}")
        return

    # ── 7. Format + split ────────────────────────────────────────────────
    full_briefing = build_briefing_from_response(claude_output, today_local)

    if not market_open_today:
        _reason = "weekend" if today_local.weekday() >= 5 else "market holiday"
        try:
            _s1, _ = get_session_dates("NYSE")
            _s1_str = _s1.strftime("%B %-d")
        except Exception:
            _s1_str = "the last trading session"
        full_briefing += (
            f"\n\n🔒 US equity markets were closed ({_reason}). "
            f"Prices reflect {_s1_str}'s close."
        )

    chunks = split_message(full_briefing)

    # ── 8. Send via Telegram ─────────────────────────────────────────────
    bot = Bot(token=bot_token)
    any_chunk_failed = False
    try:
        for chunk in chunks:
            sanitized = _sanitize_markdown(chunk)
            success = await _send_chunk_with_retry(bot, int(telegram_id), sanitized)
            if not success:
                any_chunk_failed = True
                try:
                    await bot.send_message(
                        chat_id=int(telegram_id),
                        text="Part of your briefing failed to send today. Use /brief to retry.",
                    )
                except Exception as fallback_err:
                    logger.error(
                        f"Fallback message also failed for {telegram_id}: {fallback_err}"
                    )
    finally:
        # ── 9. Persist last_briefing_date ────────────────────────────────
        # Always save regardless of send success — prevents duplicate briefings
        # on retry if Telegram had a transient error after some chunks sent.
        _save_last_briefing_date(telegram_id, today_local.isoformat())

    if not any_chunk_failed:
        logger.info(f"Briefing sent successfully for user {telegram_id}")
    else:
        logger.warning(f"One or more chunks failed for user {telegram_id}; last_briefing_date saved.")
