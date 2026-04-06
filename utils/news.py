"""
utils/news.py
Fetches market headlines and stock-specific news via NewsAPI.
"""

import os
import logging
from datetime import datetime, timedelta, timezone

import requests

logger = logging.getLogger(__name__)

NEWS_API_KEY = os.environ.get("NEWS_API_KEY", "")
NEWS_API_BASE = "https://newsapi.org/v2"

# NewsAPI free tier allows searching the past 30 days.

def _today_str() -> str:
    now = datetime.now(timezone.utc)
    hours_back = 72 if now.weekday() in (5, 6) else 24
    return (now - timedelta(hours=hours_back)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _fetch(endpoint: str, params: dict) -> dict:
    """Thin wrapper around NewsAPI with error handling."""
    if not NEWS_API_KEY:
        logger.warning("NEWS_API_KEY not set — skipping news fetch")
        return {"articles": []}
    try:
        params["apiKey"] = NEWS_API_KEY
        resp = requests.get(f"{NEWS_API_BASE}/{endpoint}", params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning(f"NewsAPI error: {e}")
        return {"articles": []}


def _clean(articles: list[dict], n: int) -> list[dict]:
    """Normalise raw NewsAPI article dicts into lightweight summary dicts."""
    cleaned = []
    for a in articles[:n]:
        title = (a.get("title") or "").split(" - ")[0].strip()  # strip source suffix
        if not title or title == "[Removed]":
            continue
        cleaned.append({
            "title": title,
            "source": (a.get("source") or {}).get("name", "Unknown"),
            "url": a.get("url", ""),
            "published_at": a.get("publishedAt", ""),
        })
    return cleaned


def get_market_headlines(n: int = 5) -> list[dict]:
    """
    Fetches top N financial news headlines from today.
    Tries preferred sources first; falls back to a broader query if needed.
    """
    data = _fetch("everything", {
        "q": "stock market OR S&P 500 OR Federal Reserve OR earnings",
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": n * 3,
        "domains": "reuters.com,cnbc.com,apnews.com,marketwatch.com,investors.com",
        "from": _today_str(),
    })
    articles = data.get("articles", [])

    # Fallback: widen search if no results from preferred domains
    if len(articles) < n:
        data = _fetch("everything", {
            "q": "stock market OR economy OR inflation OR Federal Reserve",
            "language": "en",
            "sortBy": "publishedAt",
            "pageSize": n * 3,
            "from": _today_str(),
        })
        articles = data.get("articles", [])

    return _clean(articles, n)


def get_stock_news(ticker: str, company_name: str, n: int = 3) -> list[dict]:
    """
    Fetches top N news headlines for a specific stock from today.
    Used only for outlier stocks.
    """
    # Use both ticker and shortened company name for best coverage
    short_name = company_name.split()[0] if company_name else ticker
    query = f'("{ticker}" OR "{short_name}") AND (stock OR shares OR earnings OR market)'

    data = _fetch("everything", {
        "q": query,
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": n * 3,
        "from": _today_str(),
    })
    return _clean(data.get("articles", []), n)
