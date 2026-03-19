import re
from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd

from models import Transaction


class BaseAdapter(ABC):
    @abstractmethod
    def parse(self, filepath: Path) -> list[Transaction]: ...


# ---------------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------------

def find_header_row(raw: pd.DataFrame, pattern: re.Pattern) -> int:
    """Return the index of the first row that contains a cell matching pattern."""
    for i, row in raw.iterrows():
        if any(pattern.search(str(v)) for v in row if pd.notna(v)):
            return i
    raise ValueError(f"Could not find header row matching {pattern.pattern!r}")


def find_col(columns: list[str], pattern: re.Pattern) -> str | None:
    """Return the column name matching pattern, preferring an exact (full-string) match."""
    partial = None
    for col in columns:
        if pattern.search(col):
            if pattern.fullmatch(col.strip()):
                return col
            if partial is None:
                partial = col
    return partial


def load_df(filepath: Path, header_pattern: re.Pattern) -> pd.DataFrame:
    """Load an xlsx or CSV file, auto-detecting the header row via pattern."""
    suffix = filepath.suffix.lower()
    if suffix in (".xlsx", ".xls"):
        raw = pd.read_excel(filepath, header=None, dtype=str)
        header_row = find_header_row(raw, header_pattern)
        return pd.read_excel(filepath, header=header_row, dtype=str)
    elif suffix == ".csv":
        for sep in (";", ","):
            df = pd.read_csv(filepath, sep=sep, dtype=str)
            if len(df.columns) >= 2:
                return df
        return pd.read_csv(filepath, dtype=str)
    raise ValueError(f"Unsupported file type: {suffix}")


def parse_amount(raw: str) -> int:
    """Parse Swedish or standard number formats → milliunits.

    Handles:  '-1 234,56'  '-1234.56'  '1 234.56'
    """
    cleaned = re.sub(r"[^\d.,-]", "", str(raw))
    # If both ',' and '.' present, the last one is the decimal separator
    if "," in cleaned and "." in cleaned:
        # e.g. '1.234,56' → remove dots, swap comma
        if cleaned.rindex(",") > cleaned.rindex("."):
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    else:
        cleaned = cleaned.replace(",", ".")
    return round(float(cleaned) * 1000)


def iter_rows(
    df: pd.DataFrame,
    date_col: str,
    amount_col: str,
    desc_col: str | None,
) -> list[tuple[str, str, str]]:
    """Yield (raw_date, raw_amount, raw_desc) for non-blank rows."""
    rows = []
    for _, row in df.iterrows():
        raw_date = str(row[date_col]).strip()
        raw_amount = str(row[amount_col]).strip()
        raw_desc = str(row[desc_col]).strip() if desc_col else ""
        if not raw_date or raw_date == "nan" or not raw_amount or raw_amount == "nan":
            continue
        rows.append((raw_date, raw_amount, raw_desc if raw_desc != "nan" else ""))
    return rows
