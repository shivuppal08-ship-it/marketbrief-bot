"""
prompts/weekend_roundup.py
Builds the Monday Weekend News Roundup briefing prompt.

Only sent when at least one article scores >= 4.
No price data, no educational concept, no sector breakdown.
"""


def build_weekend_roundup_prompt(
    user: dict,
    scored_articles: list[dict],  # pre-filtered to relevance_score >= 4
) -> str:
    first_name = user.get("first_name", "User")
    knowledge_level = user.get("knowledge_level", "intermediate")

    invested_tickers  = [e["ticker"] for e in user.get("invested", [])]
    watchlist_tickers = [e["ticker"] for e in user.get("watchlist", [])]
    all_tickers = ", ".join(invested_tickers + watchlist_tickers) or "(none)"

    articles_str = "\n".join(
        f"- [{a['relevance_score']}] {a['title']} ({a.get('source', '')})"
        + (f"\n  {a['description'][:120]}" if a.get("description") else "")
        for a in scored_articles[:8]
    )

    return f"""You are generating a Monday WEEKEND NEWS ROUNDUP for {first_name}.
Knowledge level: {knowledge_level}

Frame this briefing as: "Here's what broke over the weekend that could affect
your portfolio at Monday's open."

User's securities:
Owned: {", ".join(invested_tickers) or "(none)"}
Watching: {", ".join(watchlist_tickers) or "(none)"}
All tickers (for reference): {all_tickers}

Scored news articles from the weekend (relevance_score shown in brackets):
{articles_str}

Requirements:
- Surface only the news items that are materially relevant to this user's
  actual securities or the macro environment affecting them.
- For each article you include: state what happened, why it matters to this
  user specifically (reference their ticker or sector), and what to watch for
  at Monday's open.
- Maximum 8 articles. Fewer is better — only include articles with genuine
  signal.
- No sector performance section. No educational concept. No price data.
- No placeholder text if an article isn't relevant — simply skip it.
- Calibrate language and depth to {knowledge_level} level.
- Never suggest buying or selling.

GLOBAL SILENCE RULE: Do not write "no major events", "markets were quiet", or
any equivalent. Only write about what actually happened.

Output format:
📰 *WEEKEND NEWS ROUNDUP*
_What broke while markets were closed — and what it means for Monday_

─────────────────────

[For each relevant article:]
*[Headline or short title]*
[2-3 sentences: what happened, why it matters to this user, what to watch]

─────────────────────"""
