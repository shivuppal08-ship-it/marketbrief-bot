"""
utils/ticker_resolver.py

Resolves a bare ticker symbol to its correct Yahoo Finance symbol,
asset type, and Finnhub-compatible symbol using yf.Ticker.fast_info.

Module-level in-memory cache ensures each ticker is resolved only once
per process run.
"""

import logging

import yfinance as yf

logger = logging.getLogger(__name__)

# In-memory cache: bare ticker (uppercase) → resolved dict
_cache: dict[str, dict] = {}


def resolve_ticker(ticker: str, finnhub_client=None) -> dict:
    """
    Resolves a bare ticker to its canonical symbols and asset type.

    Returns a dict with:
        ticker         — bare ticker as typed (uppercased)
        yf_symbol      — correct Yahoo Finance symbol (e.g. BTC-USD, AAPL)
        asset_type     — quote_type from yfinance (e.g. CRYPTOCURRENCY, EQUITY,
                         ETF) or 'UNKNOWN' on failure
        finnhub_symbol — correct symbol for Finnhub calls
                         (e.g. BINANCE:BTCUSDT for crypto, bare ticker otherwise)

    Results are cached in-memory so the same ticker is never looked up twice.
    """
    if ticker in _cache:
        return _cache[ticker]

    try:
        quote_type = yf.Ticker(ticker).fast_info.quote_type

        if quote_type == "CRYPTOCURRENCY":
            result = {
                "ticker": ticker,
                "yf_symbol": f"{ticker}-USD",
                "asset_type": "CRYPTOCURRENCY",
                "finnhub_symbol": f"BINANCE:{ticker}USDT",
            }
        elif quote_type == "ETF":
            result = {
                "ticker": ticker,
                "yf_symbol": ticker,
                "asset_type": "ETF",
                "finnhub_symbol": ticker,
            }
        elif quote_type == "EQUITY":
            result = {
                "ticker": ticker,
                "yf_symbol": ticker,
                "asset_type": "EQUITY",
                "finnhub_symbol": ticker,
            }
        else:
            result = {
                "ticker": ticker,
                "yf_symbol": ticker,
                "asset_type": quote_type or "UNKNOWN",
                "finnhub_symbol": ticker,
            }

    except Exception as e:
        logger.warning(f"resolve_ticker failed for {ticker}: {e}")
        result = {
            "ticker": ticker,
            "yf_symbol": ticker,
            "asset_type": "UNKNOWN",
            "finnhub_symbol": ticker,
        }

    _cache[ticker] = result
    return result
