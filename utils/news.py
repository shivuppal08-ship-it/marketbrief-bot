"""
utils/news.py
Fetches market headlines and stock-specific news via NewsAPI.
"""

import os
import logging
from datetime import datetime, timedelta, timezone

import requests
import yfinance as yf

logger = logging.getLogger(__name__)

NEWS_API_KEY = os.environ.get("NEWS_API_KEY", "")
NEWS_API_BASE = "https://newsapi.org/v2"

# NewsAPI free tier allows searching the past 30 days.

# Module-level name cache: ticker → short name string
# Populated once per process run; avoids re-fetching within the same briefing.
_name_cache: dict[str, str] = {}

_MACRO_KEYWORDS: list[str] = [
    "fed", "federal reserve", "interest rates", "cpi", "inflation",
    "pce", "recession", "gdp", "unemployment", "us dollar",
    "tariffs", "trade war",
]

_SECTOR_KEYWORDS: list[str] = [
    "clarity act", "sec crypto", "stablecoin", "crypto etf",
    "export controls", "tsmc", "data center", "chip ban",
    "ai infrastructure", "oil prices", "opec", "energy policy",
    "fda approval", "clinical trial", "genomics",
    "fintech regulation", "digital payments",
]

_RELEVANCE_THRESHOLD = 4


def _today_str() -> str:
    now = datetime.now(timezone.utc)
    hours_back = 72 if now.weekday() in (5, 6) else 24
    return (now - timedelta(hours=hours_back)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _get_short_name(ticker: str) -> str:
    """Returns the cached short name for a ticker, fetching from yfinance if needed."""
    if ticker not in _name_cache:
        try:
            name = yf.Ticker(ticker).info.get("shortName") or ""
            _name_cache[ticker] = name.lower()
        except Exception:
            _name_cache[ticker] = ""
    return _name_cache[ticker]


def _build_name_cache(user: dict) -> None:
    """
    Pre-populates _name_cache for all tickers in user's invested + watchlist.
    Call once before scoring a batch of articles.
    """
    all_entries = user.get("invested", []) + user.get("watchlist", [])
    for entry in all_entries:
        ticker = entry.get("ticker", "")
        if ticker and ticker not in _name_cache:
            _get_short_name(ticker)


def score_article(article: dict, user: dict) -> dict:
    """
    Scores an article for relevance to the user's portfolio and macro context.

    Scoring (additive, capped at 10):
      +4  title/description matches an invested ticker or its company name
      +3  title/description matches a macro keyword
      +2  title/description matches a sector keyword
      +1  title/description matches a watchlist ticker or its company name

    Returns the article dict with two fields added:
      "relevance_score": int
      "matched_tiers": list[str]  e.g. ["invested:NVDA", "macro:tariffs"]
    """
    text = " ".join([
        (article.get("title") or ""),
        (article.get("description") or ""),
    ]).lower()

    score = 0
    matched: list[str] = []

    # +4 — invested tickers / company names
    for entry in user.get("invested", []):
        ticker = entry.get("ticker", "")
        name = _get_short_name(ticker)
        if (ticker and ticker.lower() in text) or (name and name in text):
            score += 4
            matched.append(f"invested:{ticker}")
            break  # only award once per tier

    # +3 — macro keywords
    for kw in _MACRO_KEYWORDS:
        if kw in text:
            score += 3
            matched.append(f"macro:{kw}")
            break

    # +2 — sector keywords
    for kw in _SECTOR_KEYWORDS:
        if kw in text:
            score += 2
            matched.append(f"sector:{kw}")
            break

    # +1 — watchlist tickers / company names
    for entry in user.get("watchlist", []):
        ticker = entry.get("ticker", "")
        name = _get_short_name(ticker)
        if (ticker and ticker.lower() in text) or (name and name in text):
            score += 1
            matched.append(f"watchlist:{ticker}")
            break

    article["relevance_score"] = min(score, 10)
    article["matched_tiers"] = matched
    return article


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
            "description": a.get("description") or "",
        })
    return cleaned


def _score_and_filter(articles: list[dict], user: dict | None, n: int) -> list[dict]:
    """
    Scores each article, drops those below the relevance threshold, returns top n.
    If no user is provided, returns articles unfiltered (no scoring context).
    """
    if not user:
        return articles[:n]

    scored = [score_article(a, user) for a in articles]
    passing = [a for a in scored if a["relevance_score"] >= _RELEVANCE_THRESHOLD]
    dropped = len(scored) - len(passing)
    if dropped:
        logger.debug(f"Dropped {dropped} articles below relevance threshold ({_RELEVANCE_THRESHOLD})")
    return passing[:n]


def get_market_headlines(n: int = 5, user: dict | None = None) -> list[dict]:
    """
    Fetches top N financial news headlines.
    If user is provided, scores and filters articles by relevance.
    """
    if user:
        _build_name_cache(user)

    data = _fetch("everything", {
        "q": "stock market OR S&P 500 OR Federal Reserve OR earnings",
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": n * 5,
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
            "pageSize": n * 5,
            "from": _today_str(),
        })
        articles = data.get("articles", [])

    cleaned = _clean(articles, len(articles))
    return _score_and_filter(cleaned, user, n)


def get_stock_news(ticker: str, company_name: str, n: int = 3, user: dict | None = None) -> list[dict]:
    """
    Fetches top N news headlines for a specific stock.
    If user is provided, scores and filters articles by relevance.
    """
    if user:
        _build_name_cache(user)

    short_name = company_name.split()[0] if company_name else ticker
    query = f'("{ticker}" OR "{short_name}") AND (stock OR shares OR earnings OR market)'

    data = _fetch("everything", {
        "q": query,
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": n * 5,
        "from": _today_str(),
    })
    cleaned = _clean(data.get("articles", []), len(data.get("articles", [])))
    return _score_and_filter(cleaned, user, n)
