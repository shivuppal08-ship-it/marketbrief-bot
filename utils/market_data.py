"""
utils/market_data.py
Fetches market data using Finnhub. All network calls wrapped in try/except.
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta

import finnhub
import pytz

logger = logging.getLogger(__name__)

# Finnhub client — lazy-initialized singleton
_finnhub_client: finnhub.Client | None = None


def _get_client() -> finnhub.Client:
    global _finnhub_client
    if _finnhub_client is None:
        api_key = os.environ.get("FINNHUB_API_KEY", "")
        _finnhub_client = finnhub.Client(api_key=api_key)
    return _finnhub_client


# ---------------------------------------------------------------------------
# Sector normalisation
# ---------------------------------------------------------------------------

SECTOR_MAP: dict[str, str] = {
    # Standard sector names (kept for backwards compat with stored watchlist data)
    "Technology": "Technology",
    "Healthcare": "Healthcare",
    "Health Care": "Healthcare",
    "Energy": "Energy",
    "Financial Services": "Financials",
    "Financials": "Financials",
    "Consumer Cyclical": "Consumer",
    "Consumer Defensive": "Consumer",
    "Consumer Discretionary": "Consumer",
    "Consumer Staples": "Consumer",
    "Industrials": "Industrials",
    "Real Estate": "Real Estate",
    "Utilities": "Utilities",
    "Basic Materials": "Materials",
    "Communication Services": "Communication",
    "ETF": "Broad Market",
    "EQUITY": "Broad Market",
    "INDEX": "Broad Market",
    "MUTUALFUND": "Broad Market",
    # Finnhub finnhubIndustry values
    "Media": "Communication",
    "Telecommunication Services": "Communication",
    "Biotechnology": "Healthcare",
    "Pharmaceuticals": "Healthcare",
    "Medical Devices": "Healthcare",
    "Semiconductors": "Technology",
    "Software": "Technology",
    "Software—Application": "Technology",
    "Software—Infrastructure": "Technology",
    "Internet Content & Information": "Technology",
    "Banks": "Financials",
    "Banks—Diversified": "Financials",
    "Insurance": "Financials",
    "Asset Management": "Financials",
    "Capital Markets": "Financials",
    "Oil & Gas": "Energy",
    "Oil & Gas E&P": "Energy",
    "Retail—Cyclical": "Consumer",
    "Automobiles": "Consumer",
    "Aerospace & Defense": "Industrials",
    "Transportation": "Industrials",
    "Electric Utilities": "Utilities",
    "Gas Utilities": "Utilities",
    "Gold": "Materials",
    "Chemicals": "Materials",
    "REIT": "Real Estate",
    "Real Estate Services": "Real Estate",
}

SECTOR_TO_ETF: dict[str, str] = {
    "Technology": "XLK",
    "Healthcare": "XLV",
    "Energy": "XLE",
    "Financials": "XLF",
    "Consumer": "XLY",
    "Industrials": "XLI",
    "Real Estate": "XLRE",
    "Utilities": "XLU",
    "Materials": "XLB",
    "Communication": "XLC",
    "Broad Market": "SPY",
}


def _normalize_sector(raw: str | None) -> str:
    if not raw:
        return "Broad Market"
    return SECTOR_MAP.get(raw, raw)


# ---------------------------------------------------------------------------
# Finnhub low-level helpers
# ---------------------------------------------------------------------------

def _quote(symbol: str) -> dict | None:
    """
    Calls Finnhub /quote for the given symbol.
    Returns the quote dict if the current price is non-zero, else None.
    Keys: c (current), d (change), dp (% change), h, l, o, pc (prev close), t.
    """
    try:
        q = _get_client().quote(symbol)
        if not q or q.get("c", 0) == 0:
            return None
        return q
    except Exception as e:
        logger.warning(f"Finnhub quote failed for {symbol}: {e}")
        return None


def _profile(symbol: str) -> dict:
    """
    Calls Finnhub /stock/profile2.
    Returns a non-empty dict for stocks; empty dict for ETFs / unknown symbols.
    """
    try:
        return _get_client().company_profile2(symbol=symbol) or {}
    except Exception as e:
        logger.warning(f"Finnhub profile failed for {symbol}: {e}")
        return {}


# ---------------------------------------------------------------------------
# Volatility classification
# ---------------------------------------------------------------------------

def classify_volatility(beta: float | None) -> str:
    if beta is None:
        return "medium"
    if beta < 0.8:
        return "low"
    if beta <= 1.5:
        return "medium"
    return "high"


# ---------------------------------------------------------------------------
# Ticker validation & enrichment (sync — run via asyncio.to_thread)
# ---------------------------------------------------------------------------

def validate_and_enrich_ticker(ticker: str) -> dict | None:
    """
    Validates a ticker and returns an enriched watchlist entry dict, or None
    if the ticker is invalid. Uses Finnhub company_profile2 + quote.
    """
    try:
        symbol = ticker.upper()
        profile = _profile(symbol)

        if not profile.get("name"):
            # Empty profile — ETF or unknown ticker.
            # Confirm existence via quote before accepting.
            q = _quote(symbol)
            if q is None:
                return None
            # Ticker exists but no profile — treat as ETF / Broad Market
            return {
                "ticker": symbol,
                "company_name": symbol,
                "sector": "Broad Market",
                "thesis": None,
                "why_added": None,
                "volatility_tier": "medium",
                "date_added": datetime.today().strftime("%Y-%m-%d"),
                "status": "holding",
            }

        company_name = profile.get("name", symbol)
        raw_industry = profile.get("finnhubIndustry", "")
        sector = _normalize_sector(raw_industry)

        # Fetch beta from basic financials for volatility classification
        beta: float | None = None
        try:
            metrics = _get_client().company_basic_financials(symbol, "all")
            beta = (metrics.get("metric") or {}).get("beta")
        except Exception:
            pass

        return {
            "ticker": symbol,
            "company_name": company_name,
            "sector": sector,
            "thesis": None,
            "why_added": None,
            "volatility_tier": classify_volatility(beta),
            "date_added": datetime.today().strftime("%Y-%m-%d"),
            "status": "holding",
        }
    except Exception as e:
        logger.warning(f"Finnhub validation error for {ticker}: {e}")
        return None


async def validate_tickers_parallel(
    tickers: list[str],
) -> tuple[list[dict], list[str]]:
    """Validates multiple tickers in parallel. Returns (valid_list, invalid_list)."""
    loop = asyncio.get_event_loop()
    tasks = [loop.run_in_executor(None, validate_and_enrich_ticker, t) for t in tickers]
    results = await asyncio.gather(*tasks)

    valid: list[dict] = []
    invalid: list[str] = []
    for ticker, result in zip(tickers, results):
        if result is not None:
            valid.append(result)
        else:
            invalid.append(ticker.upper())
    return valid, invalid


# ---------------------------------------------------------------------------
# Market-wide index data
# ---------------------------------------------------------------------------

# Finnhub free tier does not support index tickers (^GSPC, ^IXIC, ^DJI).
# Use ETF proxies: SPY ≈ S&P 500, QQQ ≈ Nasdaq, DIA ≈ Dow Jones.
_INDEX_PROXIES: dict[str, str] = {
    "sp500":  "SPY",
    "nasdaq": "QQQ",
    "dow":    "DIA",
}
_TREASURY_SYMBOL = "^TNX"


def get_index_data() -> dict:
    """
    Returns today's performance for S&P 500, Nasdaq, Dow, and 10-yr Treasury.

    S&P 500, Nasdaq, and Dow are proxied via SPY, QQQ, and DIA ETFs.
    Treasury yield change is in percentage points (basis points / 100).
    """
    results: dict = {}

    for name, symbol in _INDEX_PROXIES.items():
        q = _quote(symbol)
        if q is None:
            results[name] = None
            continue
        results[name] = {
            "symbol": symbol,
            "price": round(q["c"], 4),
            "change_pct": round(q["dp"], 2),
        }

    # 10-yr Treasury yield — may return zeros on Finnhub free tier; handle gracefully
    try:
        q = _get_client().quote(_TREASURY_SYMBOL)
        if q and q.get("c", 0) != 0:
            results["treasury_10y"] = {
                "symbol": _TREASURY_SYMBOL,
                "yield_pct": round(q["c"], 3),
                "change_pp": round(q["d"], 3),
            }
        else:
            results["treasury_10y"] = None
    except Exception as e:
        logger.warning(f"Finnhub treasury quote failed: {e}")
        results["treasury_10y"] = None

    return results


# ---------------------------------------------------------------------------
# Sector ETF data
# ---------------------------------------------------------------------------

def get_sector_data(sectors_needed: list[str]) -> dict:
    """
    Returns today's % change for each sector ETF corresponding to the user's
    watchlist sectors.  Key = ETF symbol, value = {change_pct, price}.
    """
    etf_symbols = list({SECTOR_TO_ETF.get(s, "SPY") for s in sectors_needed})
    results: dict = {}

    for symbol in etf_symbols:
        q = _quote(symbol)
        if q is None:
            results[symbol] = None
            continue
        results[symbol] = {
            "symbol": symbol,
            "price": round(q["c"], 2),
            "change_pct": round(q["dp"], 2),
        }

    return results


# ---------------------------------------------------------------------------
# Individual stock data
# ---------------------------------------------------------------------------

def get_stock_data(tickers: list[str]) -> dict:
    """
    Returns for each ticker: EOD close price, % change, ETF flag, brief_mode,
    and session_label.

    Primary source: EOD close cache (utils/eod_cache.py) built at 4:30pm ET.
    Fallback: Finnhub /quote (live intraday) with a WARNING log.

    brief_mode / session_label:
    - Before 17:30 ET → 'previous_session' / 'yesterday'
    - At/after 17:30 ET → 'current_session' / 'today'
    """
    from utils.eod_cache import load_eod_cache, is_cache_fresh

    # Determine brief_mode from current ET time
    _et = pytz.timezone("America/New_York")
    now_et = datetime.now(_et)
    if now_et.hour * 60 + now_et.minute < 17 * 60 + 30:
        brief_mode = "previous_session"
        session_label = "yesterday"
    else:
        brief_mode = "current_session"
        session_label = "today"

    # Load cache once
    cache_tickers: dict = {}
    if is_cache_fresh():
        cache = load_eod_cache()
        if cache:
            cache_tickers = cache.get("tickers", {})

    results: dict = {}

    for ticker_str in tickers:
        try:
            if ticker_str in cache_tickers:
                entry = cache_tickers[ticker_str]
                profile = _profile(ticker_str)
                is_etf_ticker = not bool(profile.get("name"))
                results[ticker_str] = {
                    "ticker": ticker_str,
                    "price": entry["session_1_close"],
                    "change_pct": entry["change_pct"],
                    "is_etf": is_etf_ticker,
                    "brief_mode": brief_mode,
                    "session_label": session_label,
                }
            else:
                if cache_tickers:
                    logger.warning(
                        f"Cache miss for {ticker_str}, falling back to Finnhub quote."
                    )
                q = _quote(ticker_str)
                if q is None:
                    results[ticker_str] = None
                    continue
                profile = _profile(ticker_str)
                is_etf_ticker = not bool(profile.get("name"))
                results[ticker_str] = {
                    "ticker": ticker_str,
                    "price": round(q["c"], 2),
                    "change_pct": round(q["dp"], 2),
                    "is_etf": is_etf_ticker,
                    "brief_mode": brief_mode,
                    "session_label": session_label,
                }

        except Exception as e:
            logger.warning(f"Failed to fetch stock data for {ticker_str}: {e}")
            results[ticker_str] = None

    return results


# ---------------------------------------------------------------------------
# Beta
# ---------------------------------------------------------------------------

def get_beta(ticker: str) -> float | None:
    """Returns beta for stocks; returns None for ETFs (no meaningful beta)."""
    try:
        profile = _profile(ticker.upper())
        if not profile.get("name"):
            return None  # ETF or unknown — no beta
        metrics = _get_client().company_basic_financials(ticker.upper(), "all")
        return (metrics.get("metric") or {}).get("beta")
    except Exception as e:
        logger.warning(f"Failed to fetch beta for {ticker}: {e}")
        return None


# ---------------------------------------------------------------------------
# Earnings calendar
# ---------------------------------------------------------------------------

def get_earnings_calendar(tickers: list[str]) -> list[dict]:
    """
    Returns upcoming earnings within 14 days for tickers in the watchlist.
    Results sorted by earnings date ascending.
    """
    today = datetime.today()
    from_date = today.strftime("%Y-%m-%d")
    to_date = (today + timedelta(days=14)).strftime("%Y-%m-%d")
    results: list[dict] = []
    client = _get_client()

    for ticker_str in tickers:
        try:
            # ETFs don't have earnings — skip if profile is empty
            profile = _profile(ticker_str)
            if not profile.get("name"):
                continue

            cal = client.earnings_calendar(_from=from_date, to=to_date, symbol=ticker_str)
            entries = (cal or {}).get("earningsCalendar") or []
            if entries:
                results.append({
                    "ticker": ticker_str,
                    "earnings_date": entries[0]["date"],
                })
        except Exception as e:
            logger.warning(f"Failed to fetch earnings for {ticker_str}: {e}")

    return sorted(results, key=lambda x: x["earnings_date"])
