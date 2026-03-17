# MarketBrief

A Telegram bot that delivers personalized daily market briefings powered by AI.

**Live bot:** [t.me/TheDailyFinanceBot](https://t.me/TheDailyFinanceBot)

---

## What It Does

MarketBrief runs on a schedule and sends each user a personalized daily market brief via Telegram. Each briefing covers:

- **Market Pulse** — broad index movements (S&P 500, Nasdaq, Dow)
- **Your Sectors** — sector-by-sector performance of the user's specific holdings
- **Outlier Alerts** — unusual moves in watchlist positions that diverge from their sector
- **Today's Concept** — a daily investing concept calibrated to the user's knowledge level
- **On the Radar** — forward-looking section covering upcoming earnings, macro events, and news relevant to their watchlist

Supports US equities, ETFs, and crypto. Delivers in morning and evening modes depending on user preference.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Bot framework | Python, [python-telegram-bot](https://python-telegram-bot.org/) |
| Brief generation | [Anthropic Claude API](https://www.anthropic.com/) |
| Company data & sector classification | [Finnhub API](https://finnhub.io/) |
| EOD close prices | [yfinance](https://github.com/ranaroussi/yfinance) |
| News scanning | [NewsAPI](https://newsapi.org/) |
| Scheduling | [APScheduler](https://apscheduler.readthedocs.io/) |
| Trading calendar | [pandas_market_calendars](https://github.com/rsheftel/pandas_market_calendars) |
| Hosting | [Render](https://render.com/) |
| User data storage | Render Persistent Disk |

---

## Architecture

```
APScheduler (_scheduler_tick, every minute)
    │
    ├─ Check each user's delivery time against current ET time
    │
    ├─ EOD close cache (built at 4:30pm ET each trading day via yfinance)
    ├─ Sector data + company profiles (Finnhub)
    ├─ Watchlist-relevant news (NewsAPI)
    │
    ├─ All data → Claude API (single streaming call per user)
    │
    └─ Personalized brief → Telegram
```

User profiles and delivery preferences are stored in JSON on a Render Persistent Disk. The EOD cache is rebuilt each trading day at market close to ensure accurate session % change calculations.

---

## Key Technical Challenges Solved

**Data accuracy** — yfinance EOD closes (unadjusted) vs Finnhub live quotes. Finnhub's `dp` field returns intraday % change, which is 0 outside market hours. Solved by building a session-close cache using yfinance and falling back to Finnhub only for live intraday data.

**Crypto ticker resolution** — built a ticker resolver using `yfinance fast_info.quote_type` to correctly classify and route BTC/ETH/SOL (→ Binance symbols) vs equities vs ETFs, avoiding wasted Finnhub calls for crypto.

**Persistent storage on ephemeral cloud** — migrated from Render's ephemeral filesystem to Persistent Disk with path-aware seeding logic (`RENDER_DISK_PATH` env var + first-run seed copy).

**Trading calendar awareness** — `pandas_market_calendars` for NYSE/NSE/crypto-aware session detection. Weekend and holiday briefings still fire, delivering a markets-closed brief with crypto prices and news instead of silently skipping.

**Deployment reliability** — APScheduler running inside the Render web service (not GitHub Actions) for precise per-minute scheduling with no cold-start gaps.

---

## What I Learned

Production debugging with real financial data APIs, cloud deployment constraints, calendar-aware scheduling, and the gap between "works locally" and "works reliably in production."

---

## What Comes Next

Active development continues. Upcoming work includes:

- **Content quality** — smarter On the Radar with event deduplication, progressive concept memory
- **Personalization depth** — holdings vs watchlist distinction, goal-driven brief framing
- **Scale reliability** — SQLite migration, staggered delivery, rate limit management

---

## Setup

### Required Environment Variables

| Variable | Description |
|---|---|
| `TELEGRAM_BOT_TOKEN` | From [@BotFather](https://t.me/BotFather) |
| `ANTHROPIC_API_KEY` | From [console.anthropic.com](https://console.anthropic.com/) |
| `FINNHUB_API_KEY` | From [finnhub.io](https://finnhub.io/) |
| `NEWS_API_KEY` | From [newsapi.org](https://newsapi.org/) |
| `RENDER_DISK_PATH` | Path to Render Persistent Disk mount (e.g. `/opt/render/project/data`) |

Copy `.env.example` to `.env` and fill in your values for local development.

### Install & Run

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python main.py
```
