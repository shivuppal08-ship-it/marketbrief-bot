"""
utils/sheets.py
Parses ticker lists from public Google Sheets links.
Requires the sheet to be set to "Anyone with link can view".
"""

import re
import logging

import requests

logger = logging.getLogger(__name__)


def _extract_sheet_id_and_gid(url: str) -> tuple[str, str | None]:
    """Extract spreadsheet ID and optional sheet gid from a Google Sheets URL."""
    # Match the spreadsheet ID
    id_match = re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", url)
    if not id_match:
        raise ValueError("Could not find spreadsheet ID in URL.")
    spreadsheet_id = id_match.group(1)

    # Optionally match the gid (specific sheet tab)
    gid_match = re.search(r"[#&?]gid=(\d+)", url)
    gid = gid_match.group(1) if gid_match else None

    return spreadsheet_id, gid


def fetch_tickers_from_sheets(url: str) -> list[str]:
    """
    Downloads a public Google Sheet as CSV and extracts tickers.

    Looks for:
    1. A column header named "Ticker" (case-insensitive) — uses that column.
    2. Fallback: first column (column A).

    Returns a list of uppercase ticker strings.
    Raises ValueError if the sheet is inaccessible or no tickers are found.
    """
    spreadsheet_id, gid = _extract_sheet_id_and_gid(url)

    csv_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv"
    if gid:
        csv_url += f"&gid={gid}"

    try:
        resp = requests.get(csv_url, timeout=15)
        resp.raise_for_status()
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code in (403, 401):
            raise ValueError(
                "Could not access the sheet. Make sure it's set to "
                "'Anyone with link can view'."
            ) from e
        raise ValueError(f"Failed to fetch sheet: {e}") from e
    except Exception as e:
        raise ValueError(f"Failed to fetch sheet: {e}") from e

    lines = resp.text.strip().splitlines()
    if not lines:
        raise ValueError("The sheet appears to be empty.")

    # Parse header row
    header = [col.strip().strip('"') for col in lines[0].split(",")]
    ticker_col_idx = 0  # default: column A

    for i, h in enumerate(header):
        if h.lower() == "ticker":
            ticker_col_idx = i
            break

    tickers: list[str] = []
    for line in lines[1:]:
        cols = [c.strip().strip('"') for c in line.split(",")]
        if ticker_col_idx < len(cols):
            val = cols[ticker_col_idx].strip().upper()
            if val and not val.startswith("#"):  # skip commented-out rows
                tickers.append(val)

    if not tickers:
        raise ValueError(
            "No tickers found in the sheet. Make sure column A or a column "
            "named 'Ticker' contains your ticker symbols."
        )

    logger.info(f"Parsed {len(tickers)} tickers from Google Sheet")
    return tickers


def parse_excel_tickers(file_path: str) -> list[str]:
    """
    Parses ticker symbols from an Excel (.xlsx) file.
    Looks for a column named 'Ticker'; falls back to column A.
    Returns a list of uppercase ticker strings.
    """
    import openpyxl  # imported here so it's only required when actually used

    try:
        wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
        ws = wb.active

        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            return []

        # Find "Ticker" column index
        header = [str(c).strip() if c is not None else "" for c in rows[0]]
        ticker_col_idx = 0
        for i, h in enumerate(header):
            if h.lower() == "ticker":
                ticker_col_idx = i
                break

        tickers: list[str] = []
        for row in rows[1:]:
            if ticker_col_idx < len(row) and row[ticker_col_idx] is not None:
                val = str(row[ticker_col_idx]).strip().upper()
                if val and val != "NONE":
                    tickers.append(val)

        wb.close()
        return tickers

    except Exception as e:
        logger.warning(f"Excel parse error: {e}")
        return []
