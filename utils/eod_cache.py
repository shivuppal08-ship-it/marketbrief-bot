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

# Always included regardless of user watchlists — broad market + all 11 sector ETFs
REQUIRED_TICKERS: list[str] = [
    # Broad market / index proxies
    "SPY", "QQQ", "DIA",
    # Sector ETFs (full SPDR suite)
    "XLK",   # Technology
    "XLC",   # Communication Services
    "XLV",   # Health Care
    "XLE",   # Energy
    "XLF",   # Financials
    "XLY",   # Consumer Discretionary
    "XLP",   # Consumer Staples
    "XLI",   # Industrials
    "XLRE",  # Real Estate
    "XLB",   # Materials
    "XLU",   # Utilities
]


def build_eod_cache(watchlist_entries: list[dict], finnhub_client) -> None:
    """
    Fetches EOD close prices for all watchlist tickers + REQUIRED_TICKERS via yfinance.

    watchlist_entries: list of watchlist dicts from users.json, each containing at
        minimum {"ticker": str} and optionally {"yf_symbol": str, "asset_type": str}.

    Uses yf_symbol for yfinance downloads (handles crypto symbols like BTC-USD).
    Cache is keyed by bare ticker so the rest of the pipeline stays unchanged.
    Crypto entries (asset_type == CRYPTOCURRENCY) are skipped — no equity calendar.
    Saves results to CACHE_FILE as JSON.
    """
    # Build {ticker: yf_symbol} mapping from watchlist entries, skip crypto
    ticker_to_yf: dict[str, str] = {}
    for entry in watchlist_entries:
        if entry.get("asset_type") == "CRYPTOCURRENCY":
            continue
        t = entry["ticker"]
        ticker_to_yf[t] = entry.get("yf_symbol", t)

    # Always include required index / sector ETFs (all equity/ETF, yf_symbol == ticker)
    for t in REQUIRED_TICKERS:
        ticker_to_yf.setdefault(t, t)

    # Group by exchange calendar using the yf_symbol for accurate detection
    # Each group maps cal_name → list of (bare_ticker, yf_symbol) pairs
    groups: dict[str | None, list[tuple[str, str]]] = {}
    for ticker, yf_sym in ticker_to_yf.items():
        cal = get_exchange_for_ticker(yf_sym, finnhub_client)
        groups.setdefault(cal, []).append((ticker, yf_sym))

    result: dict = {}

    for cal_name, ticker_yf_pairs in groups.items():
        if cal_name is None:
            # Crypto or unknown exchange — skip calendar-based EOD logic
            continue

        try:
            session_1, session_2 = get_session_dates(cal_name)
        except Exception as e:
            logger.warning(f"get_session_dates failed for {cal_name}: {e}")
            continue

        yf_symbols = [yf_sym for _, yf_sym in ticker_yf_pairs]

        logger.info(
            f"Building cache for {cal_name}: {len(yf_symbols)} tickers, "
            f"session_1={session_1}, session_2={session_2}"
        )

        try:
            if len(yf_symbols) == 1:
                raw = yf.download(
                    yf_symbols[0], period="5d", auto_adjust=False, progress=False
                )
                if raw.empty or "Close" not in raw.columns:
                    logger.warning(f"No data returned by yfinance for {yf_symbols[0]}")
                    continue
                close_map = {yf_symbols[0]: raw["Close"]}
            else:
                raw = yf.download(
                    yf_symbols, period="5d", auto_adjust=False, progress=False
                )
                if raw.empty:
                    logger.warning(f"No data returned by yfinance for {cal_name} group")
                    continue
                close_df = raw["Close"]
                close_map = {
                    sym: close_df[sym]
                    for sym in yf_symbols
                    if sym in close_df.columns
                }
        except Exception as e:
            logger.warning(f"yfinance download failed for {cal_name} group: {e}")
            continue

        for bare_ticker, yf_sym in ticker_yf_pairs:
            series = close_map.get(yf_sym)
            if series is None:
                logger.warning(f"No close series for {yf_sym} ({bare_ticker})")
                continue
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
                        f"Missing close data for {bare_ticker} ({yf_sym}): "
                        f"session_1={session_1}({s1}), session_2={session_2}({s2})"
                    )
                    continue

                # Cache keyed by bare ticker so downstream lookups stay unchanged
                result[bare_ticker] = {
                    "session_1_date": session_1.isoformat(),
                    "session_1_close": round(s1, 2),
                    "session_2_date": session_2.isoformat(),
                    "session_2_close": round(s2, 2),
                    "change_pct": round((s1 - s2) / s2 * 100, 2),
                }
            except Exception as e:
                logger.warning(f"Failed to process cache entry for {bare_ticker}: {e}")

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
