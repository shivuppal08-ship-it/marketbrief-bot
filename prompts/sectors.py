"""
prompts/sectors.py
Builds the Your Sectors section prompt.
"""

from utils.market_data import SECTOR_TO_ETF


def build_sectors_prompt(
    watchlist: list[dict],
    sector_data: dict,
    index_data: dict,
    stock_data: dict,
) -> str:
    sp = index_data.get("sp500")
    sp_str = f"{sp['change_pct']:+.2f}%" if sp else "unavailable"

    # Group watchlist items by sector
    by_sector: dict[str, list[dict]] = {}
    for w in watchlist:
        sector = w.get("sector", "Broad Market")
        by_sector.setdefault(sector, []).append(w)

    # Build per-sector block: individual stock moves + sector ETF benchmark
    sector_blocks = []
    for sector, items in by_sector.items():
        etf_symbol = SECTOR_TO_ETF.get(sector, "SPY")
        etf_perf = sector_data.get(etf_symbol)
        etf_str = f"{etf_perf['change_pct']:+.2f}%" if etf_perf else "unavailable"

        stock_lines = []
        for item in items:
            ticker = item["ticker"]
            sd = stock_data.get(ticker)
            if sd and sd.get("change_pct") is not None:
                stock_lines.append(f"  {ticker}: {sd['change_pct']:+.2f}%")
            else:
                stock_lines.append(f"  {ticker}: data unavailable")

        stocks_str = "\n".join(stock_lines)
        sector_blocks.append(
            f"{sector} (benchmark ETF {etf_symbol}: {etf_str})\n{stocks_str}"
        )

    sectors_section = "\n\n".join(sector_blocks) or "(no sector data)"

    return f"""SECTION: YOUR SECTORS

Today's individual stock performance grouped by sector:
(Benchmark ETF % shown per sector so you can compare each stock against its sector)

{sectors_section}

Broader market today: S&P 500 {sp_str}

Write the Your Sectors section of today's briefing.

Requirements:
- One paragraph or bullet per sector the user holds.
- For each sector: report each stock's actual % change (the numbers above), \
then give the primary reason that sector moved today.
- Use the sector ETF % as context to note whether individual stocks \
outperformed or underperformed their sector — but do NOT report the ETF % \
as the stock's performance.
- If a sector moved in line with the broader market, note the correlation briefly.
- If a sector diverged from the broader market, flag it and explain why.
- Do NOT dive deep into individual stocks here — that is the Outlier section's job. \
Just report each stock's number and one shared sector-level reason.
- Calibrate language and depth to the user's knowledge level.

Output format:
📂 *YOUR SECTORS*
🟢 / 🔴 / 🟡 [Sector name]: [Stock A +X%, Stock B -Y%] — [One line sector explanation]"""
