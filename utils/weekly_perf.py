"""
utils/weekly_perf.py
Calculates weekly % change for a list of securities.

Weekly change = (last Friday close - prior Friday close) / prior Friday close * 100
Uses yfinance for price data and pandas_market_calendars for correct Friday dates.
"""

import logging
from datetime import date, timedelta

import pandas_market_calendars as mcal
import yfinance as yf

logger = logging.getLogger(__name__)


def _last_two_fridays(calendar_name: str = "NYSE") -> tuple[date, date] | None:
    """
    Returns (this_friday, prev_friday) as the last two completed trading Fridays.
    Returns None if fewer than two Fridays are found in the lookback window.
    """
    try:
        today = date.today()
        start = today - timedelta(days=21)
        cal = mcal.get_calendar(calendar_name)
        schedule = cal.schedule(
            start_date=start.isoformat(),
            end_date=today.isoformat(),
        )
        trading_days = sorted(d.date() for d in schedule.index)
        fridays = [d for d in trading_days if d.weekday() == 4 and d < today]
        if len(fridays) < 2:
            return None
        return fridays[-1], fridays[-2]
    except Exception as e:
        logger.warning(f"_last_two_fridays failed: {e}")
        return None


def get_weekly_performance(entries: list[dict]) -> list[dict]:
    """
    For each watchlist/invested entry dict, calculates the weekly % change.

    Uses entry["yf_symbol"] if present, falls back to entry["ticker"].
    Returns a list of dicts:
        {
            "ticker": str,
            "yf_symbol": str,
            "change_pct": float,   # weekly % change
            "this_friday": str,    # ISO date of the recent Friday
            "prev_friday": str,    # ISO date of the prior Friday
        }

    Entries where data is unavailable are skipped with a WARNING log.
    """
    dates = _last_two_fridays()
    if not dates:
        logger.warning("weekly_perf: could not determine last two Fridays")
        return []

    this_friday, prev_friday = dates
    results: list[dict] = []

    for entry in entries:
        ticker = entry.get("ticker", "")
        yf_sym = entry.get("yf_symbol") or ticker
        if not yf_sym:
            continue

        try:
            # Download a 30-day window to guarantee both Fridays are included
            raw = yf.download(
                yf_sym,
                start=(prev_friday - timedelta(days=5)).isoformat(),
                end=(this_friday + timedelta(days=1)).isoformat(),
                auto_adjust=False,
                progress=False,
            )
            if raw.empty or "Close" not in raw.columns:
                logger.warning(f"weekly_perf: no data for {yf_sym}")
                continue

            close_col = raw["Close"]
            # Handle multi-ticker DataFrame vs single-ticker Series
            if hasattr(close_col, "columns"):
                if yf_sym in close_col.columns:
                    close_col = close_col[yf_sym]
                else:
                    logger.warning(f"weekly_perf: {yf_sym} not in downloaded columns")
                    continue

            date_map: dict[date, float] = {
                idx.date(): float(val)
                for idx, val in close_col.items()
                if hasattr(val, '__float__') and not (val != val)  # filter NaN
            }

            this_close = date_map.get(this_friday)
            prev_close = date_map.get(prev_friday)

            if this_close is None or prev_close is None or prev_close == 0:
                logger.warning(
                    f"weekly_perf: missing close for {yf_sym} — "
                    f"this_friday={this_friday}({this_close}), "
                    f"prev_friday={prev_friday}({prev_close})"
                )
                continue

            change_pct = (this_close - prev_close) / prev_close * 100
            results.append({
                "ticker": ticker,
                "yf_symbol": yf_sym,
                "change_pct": round(change_pct, 2),
                "this_friday": this_friday.isoformat(),
                "prev_friday": prev_friday.isoformat(),
            })
        except Exception as e:
            logger.warning(f"weekly_perf: failed for {yf_sym}: {e}")

    return results
