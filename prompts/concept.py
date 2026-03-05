"""
prompts/concept.py
Builds the Today's Concept section prompt.
Only called when concept_frequency matches today's date.
"""


def build_concept_prompt(
    index_data: dict,
    sector_data: dict,
    outliers: list[dict],
    user: dict,
) -> str:
    # Assemble a brief market summary for context
    sp = index_data.get("sp500")
    sp_str = f"S&P 500 {sp['change_pct']:+.2f}%" if sp else "S&P 500 data unavailable"

    outlier_summary = ""
    if outliers:
        items = [f"{o['ticker']} ({o['change_pct']:+.2f}%)" for o in outliers]
        outlier_summary = f"Notable outliers today: {', '.join(items)}."

    knowledge_level = user.get("knowledge_level", "intermediate")
    goals_summary = user.get("goals_summary", "")

    return f"""SECTION: TODAY'S CONCEPT

Today's market events summary:
- Broad market: {sp_str}
{outlier_summary}

User knowledge level: {knowledge_level}
User goals: {goals_summary}

Identify the single most teachable market concept that emerges naturally \
from today's events. Choose the concept most directly illustrated by \
something that actually happened today.

Requirements:
- Anchor the concept to today's actual market events. Never explain a \
concept in the abstract.
- Introduce and define at least one piece of financial jargon that appeared \
in today's briefing. Use the term, define it plainly, then show it in \
action through today's events.
- 5-8 lines maximum.
- End with one sentence explaining why this concept matters to a long-term \
investor specifically.
- If today was genuinely quiet, draw from the most relevant event this week \
instead. Briefly note that today was quiet.
- Calibrate depth to knowledge level:
  BEGINNER: first principles, plain English, everyday analogies
  INTERMEDIATE: causal mechanics, one layer deeper than the obvious
  ADVANCED: second-order effects, historical parallels, edge cases

Output format:
🎓 *TODAY'S CONCEPT: {{CONCEPT NAME IN CAPS}}*
[Body]"""
