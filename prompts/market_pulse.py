"""
prompts/market_pulse.py
Builds the Market Pulse section prompt.
"""


def build_market_pulse_prompt(
    index_data: dict,
    headlines: list[dict],
) -> str:
    sp = index_data.get("sp500")
    nq = index_data.get("nasdaq")
    dj = index_data.get("dow")
    tn = index_data.get("treasury_10y")

    sp_str = f"{sp['change_pct']:+.2f}%" if sp else "unavailable"
    nq_str = f"{nq['change_pct']:+.2f}%" if nq else "unavailable"
    dj_str = f"{dj['change_pct']:+.2f}%" if dj else "unavailable"

    if tn:
        tn_str = f"{tn['yield_pct']:.2f}% ({tn['change_pp']:+.3f} pp)"
    else:
        tn_str = "unavailable"

    headlines_formatted = "\n".join(
        f"- {h['title']} ({h['source']})" for h in headlines
    ) if headlines else ""

    return f"""SECTION: MARKET PULSE

Today's market data:
- S&P 500: {sp_str}
- Nasdaq: {nq_str}
- Dow Jones: {dj_str}
- 10-Year Treasury Yield: {tn_str}

Top market headlines today:
{headlines_formatted if headlines_formatted else "(none available)"}

SILENCE RULE: If no headlines are available and all index moves are under 0.3%,
do not pad the section — state plainly what is known and move on.

Write the Market Pulse section of today's briefing.

Requirements:
- 4-6 lines maximum.
- Lead with the single most important macro driver of today's market \
movement. Not a list — just the dominant force.
- Explain not just WHAT happened but WHY it moved markets, and WHY \
THAT MATTERS to a long-term equity investor.
- If all indices moved less than 0.3% with no significant news, say so \
plainly. Quiet days are a learning point: briefly explain what causes \
low-volatility sessions and why they are normal.
- Calibrate language and depth to the user's knowledge level.
- Do not mention individual stocks — that is covered in later sections.

Output format:
🌍 *MARKET PULSE*
[Body]"""
