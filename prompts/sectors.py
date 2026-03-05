"""
prompts/sectors.py
Builds the Your Sectors section prompt.
"""

from utils.market_data import SECTOR_TO_ETF


def build_sectors_prompt(
    watchlist: list[dict],
    sector_data: dict,
    index_data: dict,
) -> str:
    sp = index_data.get("sp500")
    sp_str = f"{sp['change_pct']:+.2f}%" if sp else "unavailable"

    # Group watchlist by sector
    by_sector: dict[str, list[str]] = {}
    for w in watchlist:
        sector = w.get("sector", "Broad Market")
        by_sector.setdefault(sector, []).append(w["ticker"])

    watchlist_by_sector_lines = []
    for sector, tickers in by_sector.items():
        watchlist_by_sector_lines.append(f"- {sector}: {', '.join(tickers)}")
    watchlist_by_sector = "\n".join(watchlist_by_sector_lines) or "(empty)"

    # Sector performance table
    sector_perf_lines = []
    for sector, tickers in by_sector.items():
        etf_symbol = SECTOR_TO_ETF.get(sector, "SPY")
        perf = sector_data.get(etf_symbol)
        if perf:
            sector_perf_lines.append(
                f"- {sector} ({etf_symbol}): {perf['change_pct']:+.2f}%"
            )
        else:
            sector_perf_lines.append(f"- {sector} ({etf_symbol}): data unavailable")
    sector_performance_table = "\n".join(sector_perf_lines) or "(no sector data)"

    return f"""SECTION: YOUR SECTORS

User's watchlist grouped by sector:
{watchlist_by_sector}

Today's sector performance (only sectors in user's watchlist):
{sector_performance_table}

Broader market today: S&P 500 {sp_str}

Write the Your Sectors section of today's briefing.

Requirements:
- One paragraph or bullet per sector the user holds.
- For each sector: state direction and % move, then give the single \
causal reason — what drove that sector today.
- If a sector moved in line with the broader market, explain the \
correlation briefly.
- If a sector diverged from the broader market, flag it and explain why.
- Do NOT explain individual stocks here. Individual stock moves are \
in the Outlier section.
- Calibrate language and depth to the user's knowledge level.

Output format:
📂 *YOUR SECTORS*
🟢 / 🔴 / 🟡 [Sector name] ([+/-X%]): [One line explanation]"""
