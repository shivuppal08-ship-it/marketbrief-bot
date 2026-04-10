"""
prompts/week_in_review.py
Builds the Sunday Week in Review briefing prompt.

Silence principle: sections with no meaningful content are omitted entirely.
Claude is instructed to produce no output for a section if data is absent.
"""

from datetime import datetime, timedelta


def build_week_in_review_prompt(
    user: dict,
    invested_perf: list[dict],    # from get_weekly_performance(invested)
    watchlist_perf: list[dict],   # from get_weekly_performance(watchlist)
    scored_headlines: list[dict], # articles with relevance_score >= 4
    earnings_calendar: list[dict],
) -> str:
    first_name = user.get("first_name", "User")
    knowledge_level = user.get("knowledge_level", "intermediate")

    # ── Weekly performance table ─────────────────────────────────────────
    def _fmt_perf(perf_list: list[dict], label: str) -> str:
        if not perf_list:
            return ""
        lines = [f"{label}:"]
        for p in perf_list:
            sign = "+" if p["change_pct"] >= 0 else ""
            lines.append(f"  {p['ticker']}: {sign}{p['change_pct']:.2f}%")
        return "\n".join(lines)

    invested_str  = _fmt_perf(invested_perf, "Owned")
    watchlist_str = _fmt_perf(watchlist_perf, "Watching")
    perf_block = "\n\n".join(filter(None, [invested_str, watchlist_str]))

    # Determine the date range for the label
    all_perf = invested_perf + watchlist_perf
    if all_perf:
        prev_fri = all_perf[0]["prev_friday"]
        this_fri = all_perf[0]["this_friday"]
        date_range = f"Week of {prev_fri} → {this_fri}"
    else:
        date_range = "this week"

    # ── Top movers ───────────────────────────────────────────────────────
    all_perf_sorted = sorted(
        all_perf, key=lambda x: abs(x["change_pct"]), reverse=True
    )
    top_movers = all_perf_sorted[:3]

    # Match movers to scored news for driver context
    def _find_news_for(ticker: str) -> str:
        for a in scored_headlines:
            text = (a.get("title", "") + " " + a.get("description", "")).lower()
            if ticker.lower() in text:
                return a.get("title", "")
        return ""

    top_movers_lines: list[str] = []
    for p in top_movers:
        sign = "+" if p["change_pct"] >= 0 else ""
        line = f"  {p['ticker']}: {sign}{p['change_pct']:.2f}%"
        driver = _find_news_for(p["ticker"])
        if driver:
            line += f" — {driver}"
        top_movers_lines.append(line)
    top_movers_str = "\n".join(top_movers_lines) if len(top_movers) >= 3 else ""

    # ── News themes ──────────────────────────────────────────────────────
    if scored_headlines:
        headlines_str = "\n".join(
            f"- [{a['relevance_score']}] {a['title']} ({a.get('source', '')})"
            for a in scored_headlines[:10]
        )
    else:
        headlines_str = ""

    # ── Week ahead ───────────────────────────────────────────────────────
    from prompts.radar import _get_upcoming_macro_events
    macro_events = _get_upcoming_macro_events(days_ahead=7)

    if earnings_calendar:
        earnings_str = "\n".join(
            f"- {e['ticker']}: earnings {e['earnings_date']}"
            for e in earnings_calendar
        )
    else:
        earnings_str = ""

    if macro_events:
        macro_str = "\n".join(
            f"- {e['date']}: {e['event']}" for e in macro_events
        )
    else:
        macro_str = ""

    week_ahead_str = "\n".join(filter(None, [earnings_str, macro_str]))

    # ── Assemble prompt ──────────────────────────────────────────────────
    return f"""You are generating a Sunday WEEK IN REVIEW briefing for {first_name}.
Knowledge level: {knowledge_level}

GLOBAL SILENCE RULE: If a section has no meaningful data, produce ZERO output
for that section — no header, no placeholder text, no "nothing to report."
Absence of content = absence of section.

Weekly performance data ({date_range}):
{perf_block if perf_block else "(no performance data available)"}

Top movers this week (only present if 3+ securities have data):
{top_movers_str if top_movers_str else "(insufficient data — omit top movers section)"}

Scored news articles from this week (relevance_score ≥ 4):
{headlines_str if headlines_str else "(none — omit macro themes section)"}

Week ahead — upcoming events (next 7 days):
{week_ahead_str if week_ahead_str else "(none — omit week ahead section)"}

USER'S SECURITIES:
Owned: {", ".join(p["ticker"] for p in invested_perf) or "(none with data)"}
Watching: {", ".join(p["ticker"] for p in watchlist_perf) or "(none with data)"}

Write the Week in Review briefing using ONLY the following sections, and ONLY
if each has data to report:

1. WEEKLY PERFORMANCE SUMMARY — show all securities with data, owned first
   then watching. Format each as "TICKER +X.X%" or "TICKER -X.X%".
   Group as Owned / Watching.

2. TOP 3 MOVERS — the three largest absolute % movers across both lists.
   For each, one line: ticker, %, and the driver if a news article matched.
   OMIT ENTIRELY if fewer than 3 securities have data.

3. KEY MACRO THEMES — 2-4 sentences synthesising the dominant themes from
   the week's news. Connect themes to the user's actual holdings.
   OMIT ENTIRELY if no scored articles exist.

4. WEEK AHEAD — one or two upcoming events (earnings or macro) most relevant
   to this user's specific securities. Explain what to watch for and why.
   OMIT ENTIRELY if nothing is found.

Output format for each section:
📊 *WEEKLY PERFORMANCE SUMMARY*
[body]

─────────────────────

🏆 *TOP 3 MOVERS*
[body]

─────────────────────

🌐 *KEY MACRO THEMES*
[body]

─────────────────────

📅 *WEEK AHEAD*
[body]

Calibrate language and depth to {knowledge_level} level.
Never suggest buying or selling. Frame everything as observation and learning."""
