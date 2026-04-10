"""
prompts/radar.py
Builds the On The Radar section prompt.

NOTE: Fed meeting dates and major economic releases are hardcoded here as
a static list that should be updated monthly. The spec notes these "can be
hardcoded weekly" — a future enhancement could fetch from a free calendar API
such as Finnhub or Tradier free tier.
"""

from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Hardcoded Fed & economic calendar — update these monthly
# ---------------------------------------------------------------------------
# Format: (date_str "YYYY-MM-DD", event_description)
FED_AND_MACRO_EVENTS: list[tuple[str, str]] = [
    # 2026 FOMC meetings (update when new dates announced)
    ("2026-01-28", "FOMC meeting decision (Fed interest rate decision)"),
    ("2026-03-18", "FOMC meeting decision (Fed interest rate decision)"),
    ("2026-05-06", "FOMC meeting decision (Fed interest rate decision)"),
    ("2026-06-17", "FOMC meeting decision (Fed interest rate decision)"),
    ("2026-07-29", "FOMC meeting decision (Fed interest rate decision)"),
    ("2026-09-16", "FOMC meeting decision (Fed interest rate decision)"),
    ("2026-11-04", "FOMC meeting decision (Fed interest rate decision)"),
    ("2026-12-16", "FOMC meeting decision (Fed interest rate decision)"),
]


def _get_upcoming_macro_events(days_ahead: int = 14) -> list[dict]:
    today = datetime.today().date()
    cutoff = today + timedelta(days=days_ahead)
    upcoming = []
    for date_str, desc in FED_AND_MACRO_EVENTS:
        try:
            event_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            continue
        if today <= event_date <= cutoff:
            upcoming.append({"date": date_str, "event": desc})
    return upcoming


def build_radar_prompt(
    watchlist: list[dict],
    earnings_calendar: list[dict],
) -> str:
    tickers_and_sectors = ", ".join(
        f"{w['ticker']} ({w.get('sector', '?')})" for w in watchlist
    )

    earnings_lines = "\n".join(
        f"- {e['ticker']}: earnings {e['earnings_date']}"
        for e in earnings_calendar
    ) if earnings_calendar else ""

    macro_events = _get_upcoming_macro_events()
    macro_lines = "\n".join(
        f"- {e['date']}: {e['event']}" for e in macro_events
    ) if macro_events else ""

    upcoming_block = "\n".join(filter(None, [earnings_lines, macro_lines]))

    if not upcoming_block:
        # Nothing to report — silence principle: return empty string so caller can skip
        return ""

    return f"""SECTION: ON THE RADAR

User's watchlist: {tickers_and_sectors}

Upcoming events in the next 7-14 days:
{upcoming_block}

Identify the single most relevant upcoming event for this user's specific watchlist.

SILENCE RULE: If nothing listed is genuinely relevant to this user's holdings,
produce NO output for this section — not even the header.

Requirements:
- One event only. The most relevant to their holdings.
- Explain what the event is, when it is, and why it matters to something \
specific in their watchlist.
- For earnings: name which holding is affected and why the result could \
matter.
- For macro events: explain what outcome to watch for and how each likely \
outcome affects their sector mix.
- Be specific. No vague forward-looking statements.
- Calibrate language and depth to knowledge level.

Output format:
📅 *ON THE RADAR*
[3-5 lines: event, date, why it matters to their watchlist]"""
