"""
utils/eod_cache.py
Build and serve the EOD close price cache.

build_eod_cache() is called at 4:30pm ET Mon-Fri via APScheduler.
get_stock_data() in market_data.py reads the cache before falling back to Finnhub.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf

from utils.calendar_utils import get_exchange_for_ticker, get_session_dates

logger = logging.getLogger(__name__)

CACHE_FILE = Path(__file__).parent.parent / "data" / "market_close_cache.json"

# Always include these regardless of what users hold
FIXED_TICKERS: list[str] = [
    "SPY", "QQQ", "DIA",
    "XLK", "XLC", "XLV", "XLE", "XLF", "XLY", "XLI", "XLRE",
]


def build_eod_cache(all_tickers: list[str], finnhub_client) -> None:
    """
    Fetches EOD close prices for all_tickers + FIXED_TICKERS via yfinance.
    Groups tickers by exchange calendar (NYSE / NSE / None for crypto).
    Saves results to CACHE_FILE as JSON.
    """
    tickers = list(set(all_tickers) | set(FIXED_TICKERS))

    # Group by calendar name
    groups: dict[str | None, list[str]] = {}
    for ticker in tickers:
        cal = get_exchange_for_ticker(ticker, finnhub_client)
        groups.setdefault(cal, []).append(ticker)

    result: dict = {}

    for cal_name, ticker_list in groups.items():
        if cal_name is None:
            # Crypto — skip for now (no calendar-based EOD logic)
            continue

        try:
            session_1, session_2 = get_session_dates(cal_name)
        except Exception as e:
            logger.warning(f"get_session_dates failed for {cal_name}: {e}")
            continue

        logger.info(
            f"Building cache for {cal_name}: {len(ticker_list)} tickers, "
            f"session_1={session_1}, session_2={session_2}"
        )

        try:
            if len(ticker_list) == 1:
                raw = yf.download(
                    ticker_list[0], period="5d", auto_adjust=False, progress=False
                )
                if raw.empty or "Close" not in raw.columns:
                    logger.warning(f"No data returned by yfinance for {ticker_list[0]}")
                    continue
                close_map = {ticker_list[0]: raw["Close"]}
            else:
                raw = yf.download(
                    ticker_list, period="5d", auto_adjust=False, progress=False
                )
                if raw.empty:
                    logger.warning(f"No data returned by yfinance for {cal_name} group")
                    continue
                close_df = raw["Close"]
                close_map = {
                    t: close_df[t]
                    for t in ticker_list
                    if t in close_df.columns
                }
        except Exception as e:
            logger.warning(f"yfinance download failed for {cal_name} group: {e}")
            continue

        for ticker, series in close_map.items():
            try:
                date_to_close: dict = {
                    idx.date(): float(val)
                    for idx, val in series.items()
                    if pd.notna(val)
                }
                s1 = date_to_close.get(session_1)
                s2 = date_to_close.get(session_2)

                if s1 is None or s2 is None or s2 == 0:
                    logger.warning(
                        f"Missing close data for {ticker}: "
                        f"session_1={session_1}({s1}), session_2={session_2}({s2})"
                    )
                    continue

                result[ticker] = {
                    "session_1_date": session_1.isoformat(),
                    "session_1_close": round(s1, 2),
                    "session_2_date": session_2.isoformat(),
                    "session_2_close": round(s2, 2),
                    "change_pct": round((s1 - s2) / s2 * 100, 2),
                }
            except Exception as e:
                logger.warning(f"Failed to process cache entry for {ticker}: {e}")

    cache = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "tickers": result,
    }

    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)

    logger.info(f"EOD cache built: {len(result)} tickers → {CACHE_FILE}")


def load_eod_cache() -> dict | None:
    """Reads and returns the cache dict, or None if the file doesn't exist."""
    if not CACHE_FILE.exists():
        return None
    try:
        with open(CACHE_FILE) as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to load EOD cache: {e}")
        return None


def is_cache_fresh() -> bool:
    """Returns True if the cache was generated within the last 20 hours."""
    cache = load_eod_cache()
    if not cache:
        return False
    try:
        generated_at = datetime.fromisoformat(cache["generated_at"])
        age = datetime.now(timezone.utc) - generated_at
        return age.total_seconds() < 20 * 3600
    except Exception:
        return False
