"""
prompts/security_watch.py
Builds the Security to Watch section prompt.
Only called on Fridays.
"""


def build_security_watch_prompt(user: dict) -> str:
    all_securities = user.get("invested", []) + user.get("watchlist", [])
    goals_summary = user.get("goals_summary", "")
    knowledge_level = user.get("knowledge_level", "intermediate")

    watchlist_full = "\n".join(
        f"- {w['ticker']} | {w.get('company_name', '—')} | {w.get('sector', '—')} | "
        f"thesis: {w.get('thesis') or '—'} | vol: {w.get('volatility_tier', '—')}"
        for w in all_securities
    ) or "(no securities)"

    existing_tickers = ", ".join(w["ticker"] for w in all_securities) or "none"

    return f"""SECTION: SECURITY TO WATCH

User's current watchlist:
{watchlist_full}

Tickers already in watchlist (do NOT suggest these): {existing_tickers}

User's goals: {goals_summary}
User's knowledge level: {knowledge_level}

Suggest ONE security not currently in the user's watchlist that would be \
worth researching.

Selection priority:
1. Fills a gap in their portfolio (underrepresented sector, missing \
asset class, no international exposure, no bonds, etc.)
2. Aligns with a theme already present in their thesis statements
3. Had a notable development this week that makes it worth understanding

Requirements:
- Never suggest anything already in their watchlist.
- Never frame this as a buy recommendation. Always frame as "worth researching."
- Explain: what it is, why it's specifically relevant to THIS user \
(reference their portfolio or thesis), and what to look into before \
deciding to add it.
- End with 2-3 specific research tasks before making any decision.
- If the security has meaningful risks relevant to their profile, \
flag them honestly.
- Calibrate language and depth to knowledge level.

Output format:
💡 *SECURITY TO WATCH*
*{{TICKER}} — {{Company/Fund Name}}*
[5-8 lines: what it is, why it fits this user, what to research]"""
