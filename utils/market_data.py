"""
utils/market_data.py
Fetches market data using yfinance. All network calls wrapped in try/except.
"""

import asyncio
import logging
from datetime import datetime, timedelta

import yfinance as yf

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Sector normalisation
# ---------------------------------------------------------------------------

SECTOR_MAP: dict[str, str] = {
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


def _is_etf(info: dict) -> bool:
    """Returns True if the security is an ETF, mutual fund, or index."""
    quote_type = (info.get("quoteType") or "").upper()
    return quote_type in {"ETF", "MUTUALFUND", "INDEX"}


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
    if the ticker is invalid.  Wraps yfinance — may be flaky on bad networks.

    ETFs often return 404 from the fundamentals endpoint; we fall back to
    price history to confirm the ticker exists and return a basic entry.
    """
    try:
        t = yf.Ticker(ticker.upper())

        # .info can throw 404 for ETFs — treat that as a soft failure
        try:
            info = t.info
        except Exception:
            info = {}

        if not info or not info.get("quoteType"):
            # .info unavailable — confirm ticker exists via price history
            hist = t.history(period="5d")
            if hist is None or hist.empty:
                return None
            # Ticker exists but fundamentals unavailable — treat as ETF
            return {
                "ticker": ticker.upper(),
                "company_name": ticker.upper(),
                "sector": "Broad Market",
                "thesis": None,
                "why_added": None,
                "volatility_tier": "medium",
                "date_added": datetime.today().strftime("%Y-%m-%d"),
                "status": "holding",
            }

        company_name = info.get("longName") or info.get("shortName") or ticker.upper()
        raw_sector = info.get("sector") or info.get("quoteType", "")
        sector = _normalize_sector(raw_sector)
        # ETFs don't have meaningful beta — skip to avoid 404 on fundamentals endpoint
        beta = None if _is_etf(info) else info.get("beta")
        volatility_tier = classify_volatility(beta)

        return {
            "ticker": ticker.upper(),
            "company_name": company_name,
            "sector": sector,
            "thesis": None,
            "why_added": None,
            "volatility_tier": volatility_tier,
            "date_added": datetime.today().strftime("%Y-%m-%d"),
            "status": "holding",
        }
    except Exception as e:
        logger.warning(f"yfinance validation error for {ticker}: {e}")
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
# Price change helper
# ---------------------------------------------------------------------------

def _pct_change(hist) -> float | None:
    """Return % change between last two closing prices in a history DataFrame."""
    if hist is None or len(hist) < 2:
        return None
    prev = hist["Close"].iloc[-2]
    curr = hist["Close"].iloc[-1]
    if prev == 0:
        return None
    return round(((curr - prev) / prev) * 100, 2)


# ---------------------------------------------------------------------------
# Market-wide index data
# ---------------------------------------------------------------------------

INDICES = {
    "sp500":        "^GSPC",
    "nasdaq":       "^IXIC",
    "dow":          "^DJI",
    "treasury_10y": "^TNX",
}


def get_index_data() -> dict:
    """
    Returns today's performance for S&P 500, Nasdaq, Dow, and 10-yr Treasury.

    Treasury yield is expressed as percentage points change (not % of yield),
    e.g. +0.05 means the yield rose 5 basis points.
    """
    results: dict = {}
    for name, symbol in INDICES.items():
        try:
            t = yf.Ticker(symbol)
            hist = t.history(period="2d")
            if len(hist) < 2:
                results[name] = None
                continue

            curr = round(float(hist["Close"].iloc[-1]), 4)
            prev = round(float(hist["Close"].iloc[-2]), 4)

            if name == "treasury_10y":
                # Yield is the price; change is in percentage points
                results[name] = {
                    "symbol": symbol,
                    "yield_pct": curr,
                    "change_pp": round(curr - prev, 3),
                }
            else:
                pct = round(((curr - prev) / prev) * 100, 2) if prev else 0.0
                results[name] = {
                    "symbol": symbol,
                    "price": curr,
                    "change_pct": pct,
                }
        except Exception as e:
            logger.warning(f"Failed to fetch index {symbol}: {e}")
            results[name] = None

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
        try:
            t = yf.Ticker(symbol)
            hist = t.history(period="2d")
            change = _pct_change(hist)
            if change is None:
                results[symbol] = None
                continue
            results[symbol] = {
                "symbol": symbol,
                "price": round(float(hist["Close"].iloc[-1]), 2),
                "change_pct": change,
            }
        except Exception as e:
            logger.warning(f"Failed to fetch sector ETF {symbol}: {e}")
            results[symbol] = None

    return results


# ---------------------------------------------------------------------------
# Individual stock data
# ---------------------------------------------------------------------------

def get_stock_data(tickers: list[str]) -> dict:
    """
    Returns for each ticker: today's % change and current price.
    For stocks, also returns volume vs 30-day average volume.
    For ETFs, fundamentals endpoints are skipped — only price/performance is fetched.
    """
    results: dict = {}
    for ticker_str in tickers:
        try:
            t = yf.Ticker(ticker_str)

            # Lightweight ETF detection via fast_info (avoids fundamentals 404)
            is_etf_ticker = False
            try:
                qt = (getattr(t.fast_info, "quote_type", None) or "").upper()
                is_etf_ticker = qt in {"ETF", "MUTUALFUND", "INDEX"}
            except Exception:
                pass

            hist_30 = t.history(period="30d")
            if hist_30 is None or len(hist_30) < 2:
                results[ticker_str] = None
                continue

            curr_close = float(hist_30["Close"].iloc[-1])
            prev_close = float(hist_30["Close"].iloc[-2])
            change_pct = round(((curr_close - prev_close) / prev_close) * 100, 2) if prev_close else 0.0

            entry = {
                "ticker": ticker_str,
                "price": round(curr_close, 2),
                "change_pct": change_pct,
                "is_etf": is_etf_ticker,
            }

            if not is_etf_ticker:
                # Volume ratio is stock-specific; skip for ETFs
                curr_vol = int(hist_30["Volume"].iloc[-1])
                avg_vol = int(hist_30["Volume"].mean()) if not hist_30.empty else curr_vol
                entry["volume"] = curr_vol
                entry["avg_volume"] = avg_vol
                entry["volume_ratio"] = round(curr_vol / avg_vol, 2) if avg_vol > 0 else 1.0

            results[ticker_str] = entry
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
        t = yf.Ticker(ticker)
        # Use fast_info for lightweight ETF check before hitting fundamentals endpoint
        try:
            qt = (getattr(t.fast_info, "quote_type", None) or "").upper()
            if qt in {"ETF", "MUTUALFUND", "INDEX"}:
                return None
        except Exception:
            pass
        info = t.info
        return info.get("beta")
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
    cutoff = datetime.today() + timedelta(days=14)
    results: list[dict] = []

    for ticker_str in tickers:
        try:
            t = yf.Ticker(ticker_str)

            # ETFs don't have earnings calendars — skip them
            try:
                qt = (getattr(t.fast_info, "quote_type", None) or "").upper()
                if qt in {"ETF", "MUTUALFUND", "INDEX"}:
                    continue
            except Exception:
                pass

            cal = t.calendar

            if cal is None:
                continue

            # yfinance returns different formats across versions
            if isinstance(cal, dict):
                earnings_dates = cal.get("Earnings Date", [])
            elif hasattr(cal, "to_dict"):
                d = cal.to_dict()
                earnings_dates = d.get("Earnings Date", [])
            else:
                continue

            if not isinstance(earnings_dates, list):
                earnings_dates = [earnings_dates]

            for ed in earnings_dates:
                if ed is None:
                    continue
                if isinstance(ed, str):
                    try:
                        ed = datetime.strptime(ed[:10], "%Y-%m-%d")
                    except ValueError:
                        continue
                if hasattr(ed, "to_pydatetime"):
                    ed = ed.to_pydatetime()
                if isinstance(ed, datetime) and ed.replace(tzinfo=None) <= cutoff:
                    results.append({
                        "ticker": ticker_str,
                        "earnings_date": ed.strftime("%Y-%m-%d"),
                    })
                    break  # only first upcoming date per ticker

        except Exception as e:
            logger.warning(f"Failed to fetch earnings for {ticker_str}: {e}")

    return sorted(results, key=lambda x: x["earnings_date"])
