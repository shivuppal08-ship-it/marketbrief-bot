"""
utils/ticker_resolver.py

Resolves a bare ticker symbol to its correct Yahoo Finance symbol,
asset type, and Finnhub-compatible symbol using the yfinance Lookup API.

Module-level in-memory cache ensures each ticker is resolved only once
per process run.
"""

import logging

import yfinance as yf

logger = logging.getLogger(__name__)

# In-memory cache: bare ticker (uppercase) → resolved dict
_cache: dict[str, dict] = {}

# Asset type constants
CRYPTOCURRENCY = "CRYPTOCURRENCY"
EQUITY = "EQUITY"
ETF = "ETF"
CURRENCY = "CURRENCY"
UNKNOWN = "UNKNOWN"


def resolve_ticker(ticker: str, finnhub_client) -> dict:
    """
    Resolves a bare ticker to its canonical symbols and asset type.

    Returns a dict with:
        ticker        — bare ticker as typed (uppercased)
        yf_symbol     — correct Yahoo Finance symbol (e.g. BTC-USD, AAPL)
        asset_type    — one of CRYPTOCURRENCY, EQUITY, ETF, CURRENCY, UNKNOWN
        finnhub_symbol — correct symbol for Finnhub calls
                         (e.g. BINANCE:BTCUSDT for crypto, bare ticker otherwise)

    Results are cached in-memory so the same ticker is never looked up twice.
    """
    upper = ticker.upper().strip()

    if upper in _cache:
        return _cache[upper]

    result = _resolve(upper, finnhub_client)
    _cache[upper] = result
    return result


def _resolve(upper: str, finnhub_client) -> dict:
    # ── 1. Cryptocurrency ────────────────────────────────────────────────
    try:
        hits = yf.Lookup(upper).get_cryptocurrency(count=1)
        if hits is not None and not hits.empty:
            sym = str(hits.iloc[0].get("symbol", ""))
            if upper in sym.upper():
                entry = {
                    "ticker": upper,
                    "yf_symbol": sym,
                    "asset_type": CRYPTOCURRENCY,
                    "finnhub_symbol": f"BINANCE:{upper}USDT",
                }
                logger.info(f"Resolved {upper} → {sym} (CRYPTOCURRENCY)")
                return entry
    except Exception as e:
        logger.debug(f"Crypto lookup failed for {upper}: {e}")

    # ── 2. ETF ───────────────────────────────────────────────────────────
    try:
        hits = yf.Lookup(upper).get_etf(count=1)
        if hits is not None and not hits.empty:
            sym = str(hits.iloc[0].get("symbol", ""))
            if upper in sym.upper():
                entry = {
                    "ticker": upper,
                    "yf_symbol": upper,
                    "asset_type": ETF,
                    "finnhub_symbol": upper,
                }
                logger.info(f"Resolved {upper} → {upper} (ETF)")
                return entry
    except Exception as e:
        logger.debug(f"ETF lookup failed for {upper}: {e}")

    # ── 3. Stock / Equity ────────────────────────────────────────────────
    try:
        hits = yf.Lookup(upper).get_stock(count=1)
        if hits is not None and not hits.empty:
            sym = str(hits.iloc[0].get("symbol", ""))
            if upper in sym.upper():
                entry = {
                    "ticker": upper,
                    "yf_symbol": upper,
                    "asset_type": EQUITY,
                    "finnhub_symbol": upper,
                }
                logger.info(f"Resolved {upper} → {upper} (EQUITY)")
                return entry
    except Exception as e:
        logger.debug(f"Stock lookup failed for {upper}: {e}")

    # ── 4. Unknown ───────────────────────────────────────────────────────
    logger.warning(f"Could not resolve ticker via yf.Lookup: {upper}")
    return {
        "ticker": upper,
        "yf_symbol": upper,
        "asset_type": UNKNOWN,
        "finnhub_symbol": upper,
    }
