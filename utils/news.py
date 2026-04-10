"""
utils/news.py
Fetches market headlines and stock-specific news from multiple sources:
  - NewsAPI
  - Yahoo Finance RSS (per ticker)
  - Reuters RSS
  - Marketaux API (optional)
  - Alpha Vantage News Sentiment API (optional)

All results are normalised into the same article dict format, scored for
relevance, and deduplicated before being returned.
"""

import os
import re
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

import requests
import yfinance as yf

logger = logging.getLogger(__name__)

NEWS_API_KEY        = os.environ.get("NEWS_API_KEY", "")
MARKETAUX_API_KEY   = os.environ.get("MARKETAUX_API_KEY", "")
ALPHAVANTAGE_API_KEY = os.environ.get("ALPHAVANTAGE_API_KEY", "")

NEWS_API_BASE        = "https://newsapi.org/v2"
MARKETAUX_BASE       = "https://api.marketaux.com/v1/news/all"
ALPHAVANTAGE_BASE    = "https://www.alphavantage.co/query"

# Module-level name cache: ticker → lowercase short name
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

_HTTP_TIMEOUT = 10


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------

def _today_str() -> str:
    now = datetime.now(timezone.utc)
    hours_back = 72 if now.weekday() in (5, 6) else 24
    return (now - timedelta(hours=hours_back)).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Name cache
# ---------------------------------------------------------------------------

def _get_short_name(ticker: str) -> str:
    if ticker not in _name_cache:
        try:
            name = yf.Ticker(ticker).info.get("shortName") or ""
            _name_cache[ticker] = name.lower()
        except Exception:
            _name_cache[ticker] = ""
    return _name_cache[ticker]


def _build_name_cache(user: dict) -> None:
    all_entries = user.get("invested", []) + user.get("watchlist", [])
    for entry in all_entries:
        ticker = entry.get("ticker", "")
        if ticker and ticker not in _name_cache:
            _get_short_name(ticker)


# ---------------------------------------------------------------------------
# Relevance scoring
# ---------------------------------------------------------------------------

def score_article(article: dict, user: dict) -> dict:
    """
    Scores an article for relevance to the user's portfolio and macro context.
    Additive, capped at 10. Attaches relevance_score and matched_tiers.
    """
    text = " ".join([
        (article.get("title") or ""),
        (article.get("description") or ""),
    ]).lower()

    score = 0
    matched: list[str] = []

    for entry in user.get("invested", []):
        ticker = entry.get("ticker", "")
        name = _get_short_name(ticker)
        if (ticker and ticker.lower() in text) or (name and name in text):
            score += 4
            matched.append(f"invested:{ticker}")
            break

    for kw in _MACRO_KEYWORDS:
        if kw in text:
            score += 3
            matched.append(f"macro:{kw}")
            break

    for kw in _SECTOR_KEYWORDS:
        if kw in text:
            score += 2
            matched.append(f"sector:{kw}")
            break

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


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

_PUNCT_RE = re.compile(r"[^\w\s]")


def _dedup(articles: list[dict]) -> list[dict]:
    """Deduplicate by normalised title (lowercase, punctuation stripped)."""
    seen: set[str] = set()
    out: list[dict] = []
    for a in articles:
        key = _PUNCT_RE.sub("", (a.get("title") or "").lower()).strip()
        if key and key not in seen:
            seen.add(key)
            out.append(a)
    return out


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------

def _make_article(title: str, source: str, url: str, published_at: str,
                  description: str = "") -> dict:
    title = title.split(" - ")[0].strip()
    if not title or title == "[Removed]":
        return {}
    return {
        "title": title,
        "source": source,
        "url": url,
        "published_at": published_at,
        "description": description,
    }


# ---------------------------------------------------------------------------
# NewsAPI
# ---------------------------------------------------------------------------

def _newsapi_fetch(endpoint: str, params: dict) -> dict:
    if not NEWS_API_KEY:
        logger.warning("NEWS_API_KEY not set — skipping NewsAPI fetch")
        return {"articles": []}
    try:
        params["apiKey"] = NEWS_API_KEY
        resp = requests.get(f"{NEWS_API_BASE}/{endpoint}", params=params, timeout=_HTTP_TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning(f"NewsAPI error: {e}")
        return {"articles": []}


def _newsapi_clean(articles: list[dict]) -> list[dict]:
    out = []
    for a in articles:
        article = _make_article(
            title=a.get("title") or "",
            source=(a.get("source") or {}).get("name", "Unknown"),
            url=a.get("url") or "",
            published_at=a.get("publishedAt") or "",
            description=a.get("description") or "",
        )
        if article:
            out.append(article)
    return out


def _get_newsapi_headlines(n: int) -> list[dict]:
    data = _newsapi_fetch("everything", {
        "q": "stock market OR S&P 500 OR Federal Reserve OR earnings",
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": n * 5,
        "domains": "reuters.com,cnbc.com,apnews.com,marketwatch.com,investors.com",
        "from": _today_str(),
    })
    articles = data.get("articles", [])
    if len(articles) < n:
        data = _newsapi_fetch("everything", {
            "q": "stock market OR economy OR inflation OR Federal Reserve",
            "language": "en",
            "sortBy": "publishedAt",
            "pageSize": n * 5,
            "from": _today_str(),
        })
        articles = data.get("articles", [])
    return _newsapi_clean(articles)


# ---------------------------------------------------------------------------
# Yahoo Finance RSS
# ---------------------------------------------------------------------------

def get_yahoo_finance_rss(tickers: list[str], n: int) -> list[dict]:
    """Fetches Yahoo Finance RSS headlines for a list of tickers."""
    out: list[dict] = []
    for ticker in tickers:
        url = (
            f"https://feeds.finance.yahoo.com/rss/2.0/headline"
            f"?s={ticker}&region=US&lang=en-US"
        )
        try:
            resp = requests.get(url, timeout=_HTTP_TIMEOUT, headers={"User-Agent": "Mozilla/5.0"})
            resp.raise_for_status()
            root = ET.fromstring(resp.content)
            ns = {"media": "http://search.yahoo.com/mrss/"}
            for item in root.findall(".//item"):
                title = (item.findtext("title") or "").strip()
                link  = (item.findtext("link") or "").strip()
                pub   = (item.findtext("pubDate") or "").strip()
                desc  = (item.findtext("description") or "").strip()
                article = _make_article(title, "Yahoo Finance", link, pub, desc)
                if article:
                    out.append(article)
        except Exception as e:
            logger.debug(f"Yahoo Finance RSS failed for {ticker}: {e}")
    return out


# ---------------------------------------------------------------------------
# Reuters RSS
# ---------------------------------------------------------------------------

def get_reuters_rss(n: int) -> list[dict]:
    """Fetches Reuters business news RSS feed."""
    url = "https://feeds.reuters.com/reuters/businessNews"
    out: list[dict] = []
    try:
        resp = requests.get(url, timeout=_HTTP_TIMEOUT, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        for item in root.findall(".//item"):
            title = (item.findtext("title") or "").strip()
            link  = (item.findtext("link") or "").strip()
            pub   = (item.findtext("pubDate") or "").strip()
            desc  = (item.findtext("description") or "").strip()
            article = _make_article(title, "Reuters", link, pub, desc)
            if article:
                out.append(article)
    except Exception as e:
        logger.debug(f"Reuters RSS failed: {e}")
    return out


# ---------------------------------------------------------------------------
# Marketaux
# ---------------------------------------------------------------------------

def get_marketaux(tickers: list[str], n: int) -> list[dict]:
    """Fetches news from Marketaux for the given tickers."""
    if not MARKETAUX_API_KEY:
        return []
    out: list[dict] = []
    try:
        symbols = ",".join(tickers[:10])  # API limit
        resp = requests.get(
            MARKETAUX_BASE,
            params={
                "api_token": MARKETAUX_API_KEY,
                "symbols": symbols,
                "filter_entities": "true",
                "limit": n * 3,
                "language": "en",
            },
            timeout=_HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        for item in (resp.json().get("data") or []):
            article = _make_article(
                title=item.get("title") or "",
                source=item.get("source") or "Marketaux",
                url=item.get("url") or "",
                published_at=item.get("published_at") or "",
                description=item.get("description") or "",
            )
            if article:
                out.append(article)
    except Exception as e:
        logger.debug(f"Marketaux fetch failed: {e}")
    return out


# ---------------------------------------------------------------------------
# Alpha Vantage News Sentiment
# ---------------------------------------------------------------------------

def get_alpha_vantage_news(tickers: list[str], n: int) -> list[dict]:
    """Fetches news sentiment from Alpha Vantage for the given tickers."""
    if not ALPHAVANTAGE_API_KEY:
        return []
    out: list[dict] = []
    try:
        symbols = ",".join(tickers[:10])
        resp = requests.get(
            ALPHAVANTAGE_BASE,
            params={
                "function": "NEWS_SENTIMENT",
                "tickers": symbols,
                "apikey": ALPHAVANTAGE_API_KEY,
                "limit": n * 3,
            },
            timeout=_HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        for item in (resp.json().get("feed") or []):
            article = _make_article(
                title=item.get("title") or "",
                source=item.get("source") or "Alpha Vantage",
                url=item.get("url") or "",
                published_at=item.get("time_published") or "",
                description=item.get("summary") or "",
            )
            if article:
                out.append(article)
    except Exception as e:
        logger.debug(f"Alpha Vantage news fetch failed: {e}")
    return out


# ---------------------------------------------------------------------------
# Score + filter
# ---------------------------------------------------------------------------

def _score_and_filter(articles: list[dict], user: dict | None, n: int) -> list[dict]:
    if not user:
        return articles[:n]

    scored = [score_article(a, user) for a in articles]
    passing = [a for a in scored if a["relevance_score"] >= _RELEVANCE_THRESHOLD]
    dropped = len(scored) - len(passing)
    if dropped:
        logger.debug(f"Dropped {dropped} articles below relevance threshold ({_RELEVANCE_THRESHOLD})")
    passing.sort(key=lambda a: a["relevance_score"], reverse=True)
    return passing[:n]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_market_headlines(n: int = 5, user: dict | None = None) -> list[dict]:
    """
    Fetches headlines from all configured sources, deduplicates, scores,
    filters by relevance, and returns the top N articles.
    """
    if user:
        _build_name_cache(user)

    invested_tickers = [e["ticker"] for e in (user or {}).get("invested", [])]

    all_articles: list[dict] = []
    all_articles.extend(_get_newsapi_headlines(n * 5))
    all_articles.extend(get_reuters_rss(n * 3))
    if invested_tickers:
        all_articles.extend(get_yahoo_finance_rss(invested_tickers, n * 2))
        all_articles.extend(get_marketaux(invested_tickers, n * 2))
        all_articles.extend(get_alpha_vantage_news(invested_tickers, n * 2))

    deduped = _dedup(all_articles)
    return _score_and_filter(deduped, user, n)


def get_stock_news(ticker: str, company_name: str, n: int = 3,
                   user: dict | None = None) -> list[dict]:
    """
    Fetches news for a specific ticker from NewsAPI and Yahoo Finance RSS.
    Scores and filters if user context is provided.
    """
    if user:
        _build_name_cache(user)

    short_name = company_name.split()[0] if company_name else ticker
    query = f'("{ticker}" OR "{short_name}") AND (stock OR shares OR earnings OR market)'

    data = _newsapi_fetch("everything", {
        "q": query,
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": n * 5,
        "from": _today_str(),
    })
    all_articles: list[dict] = _newsapi_clean(data.get("articles", []))
    all_articles.extend(get_yahoo_finance_rss([ticker], n * 2))

    deduped = _dedup(all_articles)
    return _score_and_filter(deduped, user, n)
