"""
prompts/outlier.py
Builds the Outlier Alert section prompt.
Only called when outliers have been detected.
"""

from utils.market_data import SECTOR_TO_ETF


def build_outlier_prompt(
    watchlist: list[dict],
    stock_data: dict,
    sector_data: dict,
    outliers: list[dict],
    outlier_news: dict,
) -> str:
    # Full stock performance table for context
    stock_lines = []
    for w in watchlist:
        t = w["ticker"]
        d = stock_data.get(t)
        if d:
            stock_lines.append(
                f"- {t} ({w.get('company_name', t)}): {d['change_pct']:+.2f}%  "
                f"| vol ratio: {d['volume_ratio']:.1f}x avg"
            )
        else:
            stock_lines.append(f"- {t}: data unavailable")
    stock_table = "\n".join(stock_lines) or "(no data)"

    # Sector averages
    sector_avg_lines = []
    seen_etfs: set = set()
    for w in watchlist:
        etf = SECTOR_TO_ETF.get(w.get("sector", ""), "SPY")
        if etf in seen_etfs:
            continue
        seen_etfs.add(etf)
        d = sector_data.get(etf)
        if d:
            sector_avg_lines.append(f"- {w.get('sector', etf)} ({etf}): {d['change_pct']:+.2f}%")
    sector_avgs = "\n".join(sector_avg_lines) or "(no sector data)"

    # Outlier list
    outlier_lines = []
    for o in outliers:
        outlier_lines.append(
            f"- {o['ticker']}: {o['change_pct']:+.2f}% "
            f"(threshold: {o['volatility_tier']} tier | sector avg: {o['sector_change_pct']:+.2f}%)"
        )
    outliers_formatted = "\n".join(outlier_lines)

    # News for each outlier
    news_lines = []
    for o in outliers:
        ticker = o["ticker"]
        news = outlier_news.get(ticker, [])
        if news:
            news_lines.append(f"{ticker}:")
            for n in news:
                news_lines.append(f"  - {n['title']} ({n['source']})")
        else:
            news_lines.append(f"{ticker}: No news headlines found.")
    outlier_news_str = "\n".join(news_lines)

    # Watchlist theses for reference
    thesis_lines = []
    for o in outliers:
        for w in watchlist:
            if w["ticker"] == o["ticker"] and w.get("thesis"):
                thesis_lines.append(f"- {o['ticker']}: {w['thesis']}")
                break

    return f"""SECTION: OUTLIER ALERT

User's watchlist with today's price changes:
{stock_table}

Volatility thresholds:
- Low volatility tier: flag if |move| > 3%
- Medium volatility tier: flag if |move| > 5%
- High volatility tier: flag if |move| > 8%

Today's sector averages:
{sector_avgs}

Outliers identified (stocks exceeding their threshold AND diverging from sector average):
{outliers_formatted}

News for outlier stocks:
{outlier_news_str}

Write the Outlier Alert section.

For each outlier:
- State ticker, % move, and sector average move today.
- Explain the specific catalyst. Be precise — not "investors reacted \
positively" but what the actual news or event was.
- Reference the user's thesis for that stock if available, and state clearly: \
does today's move SUPPORT, CONTRADICT, or have NO BEARING on their thesis?
- Calibrate language and depth to knowledge level.

Output format:
⚡ *OUTLIER ALERT*
*{{TICKER}} {{+/-X%}}* vs {{sector}} average {{+/-X%}}
[3-5 lines: catalyst + thesis check]"""
