"""
utils/calendar_utils.py
Calendar-aware trading day utilities using pandas_market_calendars.
"""

import logging
from datetime import datetime, timedelta, date

import pandas_market_calendars as mcal
import pytz

logger = logging.getLogger(__name__)

_ET = pytz.timezone("America/New_York")
_EOD_CUTOFF_MINUTES = 17 * 60 + 30  # 17:30 ET


def get_exchange_for_ticker(ticker: str, finnhub_client) -> str | None:
    """
    Returns the pandas_market_calendars calendar name for a ticker.

    Mapping rules:
    - Tickers ending in '-USD' (crypto) → None
    - Finnhub exchange containing 'NSE' or 'BSE' → 'NSE'
    - Everything else → 'NYSE'
    """
    if "-USD" in ticker.upper():
        return None

    try:
        profile = finnhub_client.company_profile2(symbol=ticker) or {}
        exchange = profile.get("exchange", "")
        if "NSE" in exchange or "BSE" in exchange:
            return "NSE"
    except Exception as e:
        logger.warning(f"Could not get exchange for {ticker}: {e}")

    return "NYSE"


def get_session_dates(calendar_name: str) -> tuple[date, date]:
    """
    Returns (session_1_date, session_2_date) as datetime.date objects.

    session_1: the session being reported on.
    session_2: the prior session used as the % change baseline.

    Logic (all times in ET):
    - Before 17:30 ET → session_1 = last completed trading day before today,
                         session_2 = the trading day before that.
    - At/after 17:30 ET → session_1 = today if a trading day, else last completed;
                           session_2 = the trading day before session_1.

    Uses pandas_market_calendars — never infers trading days from data availability.
    """
    now_et = datetime.now(_ET)
    today = now_et.date()

    calendar = mcal.get_calendar(calendar_name)
    start = today - timedelta(days=15)
    schedule = calendar.schedule(
        start_date=start.isoformat(),
        end_date=today.isoformat(),
    )
    trading_days: list[date] = sorted(d.date() for d in schedule.index)

    current_minutes = now_et.hour * 60 + now_et.minute

    if current_minutes < _EOD_CUTOFF_MINUTES:
        # Before 17:30 — report on the last completed session
        past_days = [d for d in trading_days if d < today]
        if len(past_days) < 2:
            raise ValueError(
                f"Not enough past trading days for {calendar_name} "
                f"(found {len(past_days)}, need 2)"
            )
        session_1 = past_days[-1]
        session_2 = past_days[-2]
    else:
        # At/after 17:30 — today's session has closed
        if today in trading_days:
            session_1 = today
        else:
            past = [d for d in trading_days if d <= today]
            if not past:
                raise ValueError(f"No past trading days found for {calendar_name}")
            session_1 = past[-1]

        days_before = [d for d in trading_days if d < session_1]
        if not days_before:
            raise ValueError(
                f"No prior trading day found before {session_1} for {calendar_name}"
            )
        session_2 = days_before[-1]

    return session_1, session_2


def is_trading_day(calendar_name: str, check_date: date) -> bool:
    """
    Returns True if check_date is a valid trading session for the given calendar.
    Defaults to True on error to avoid blocking briefings.
    """
    try:
        calendar = mcal.get_calendar(calendar_name)
        schedule = calendar.schedule(
            start_date=check_date.isoformat(),
            end_date=check_date.isoformat(),
        )
        return len(schedule) > 0
    except Exception as e:
        logger.warning(
            f"is_trading_day check failed for {calendar_name} on {check_date}: {e}"
        )
        return True
