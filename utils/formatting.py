"""
utils/formatting.py
Telegram markdown formatting helpers and briefing assembly.
"""

import re
from datetime import datetime


DIVIDER = "─────────────────────"


def bold(text: str) -> str:
    return f"*{text}*"


def section_divider() -> str:
    return f"\n{DIVIDER}\n"


def escape_md(text: str) -> str:
    """Escape special characters for Telegram MarkdownV2."""
    special = r"_*[]()~`>#+-=|{}.!"
    return "".join(f"\\{c}" if c in special else c for c in str(text))


def build_full_briefing(sections: list[str], date: datetime) -> str:
    """
    Assembles a list of pre-generated section strings into one Telegram message.
    Matches the spec's formatting.py interface.
    """
    day_name = date.strftime("%A")
    date_str = f"{date.strftime('%B')} {date.day}, {date.year}"
    header = f"📊 *Market Brief — {day_name}, {date_str}*"

    non_empty = [s.strip() for s in sections if s and s.strip()]

    parts = [header]
    for section in non_empty:
        parts.append(DIVIDER)
        parts.append(section)

    return "\n".join(parts)


def build_briefing_from_response(claude_output: str, date: datetime) -> str:
    """
    Wraps a single-call Claude response in the standard briefing header.
    Used when all sections are generated in one API call.
    """
    day_name = date.strftime("%A")
    date_str = f"{date.strftime('%B')} {date.day}, {date.year}"
    header = f"📊 *Market Brief — {day_name}, {date_str}*\n{DIVIDER}"
    return f"{header}\n{claude_output.strip()}"


def split_message(text: str, max_length: int = 4096) -> list[str]:
    """
    Splits a long Telegram message into chunks ≤ max_length.
    Splits preferentially at section dividers, then at double newlines,
    then hard-splits as a last resort.
    """
    if len(text) <= max_length:
        return [text]

    chunks: list[str] = []
    remaining = text

    while len(remaining) > max_length:
        # Try to split at a divider
        split_at = remaining.rfind(DIVIDER, 0, max_length)

        if split_at <= 0:
            # Try double newline
            split_at = remaining.rfind("\n\n", 0, max_length)

        if split_at <= 0:
            # Hard split
            split_at = max_length

        chunks.append(remaining[:split_at].strip())
        remaining = remaining[split_at:].strip()

    if remaining:
        chunks.append(remaining)

    return chunks
