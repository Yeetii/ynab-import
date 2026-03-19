import re
from datetime import date
from pathlib import Path

import pandas as pd

from adapters.base import BaseAdapter
from models import Transaction

BANK = "spendwise"

# Header keyword patterns for flexible column detection
_DATE_PATTERNS = re.compile(r"\b(date|datum)\b", re.I)
_AMOUNT_PATTERNS = re.compile(r"\b(amount|belopp|sum)\b", re.I)
_DESC_PATTERNS = re.compile(r"\b(description|text|payee|memo|kommentar|beskrivning|specifikation)\b", re.I)


def _find_col(columns: list[str], pattern: re.Pattern) -> str | None:
    # Prefer exact (full-string) match over partial match
    partial = None
    for col in columns:
        if pattern.search(col):
            if pattern.fullmatch(col.strip()):
                return col
            if partial is None:
                partial = col
    return partial


def _parse_amount(raw: str) -> int:
    """Parse various number formats → milliunits."""
    cleaned = re.sub(r"[^\d.,-]", "", str(raw))
    cleaned = cleaned.replace(",", ".")
    return round(float(cleaned) * 1000)


class SpendwiseAdapter(BaseAdapter):
    def parse(self, filepath: Path) -> list[Transaction]:
        suffix = filepath.suffix.lower()
        if suffix in (".xlsx", ".xls"):
            # Spendwise exports have metadata rows before the actual header;
            # scan for the real header row by looking for a date-like column.
            raw = pd.read_excel(filepath, header=None, dtype=str)
            header_row = 0
            for i, row in raw.iterrows():
                if any(_DATE_PATTERNS.search(str(v)) for v in row if pd.notna(v)):
                    header_row = i
                    break
            df = pd.read_excel(filepath, header=header_row, dtype=str)
        elif suffix == ".csv":
            # Try semicolon first (common in Swedish exports), fall back to comma
            try:
                df = pd.read_csv(filepath, sep=";", dtype=str)
                if len(df.columns) < 2:
                    df = pd.read_csv(filepath, sep=",", dtype=str)
            except Exception:
                df = pd.read_csv(filepath, dtype=str)
        else:
            raise ValueError(f"Unsupported file type: {suffix}")

        df.columns = [c.strip() for c in df.columns]
        cols = list(df.columns)

        date_col = _find_col(cols, _DATE_PATTERNS)
        amount_col = _find_col(cols, _AMOUNT_PATTERNS)
        desc_col = _find_col(cols, _DESC_PATTERNS)

        missing = [name for name, col in [("date", date_col), ("amount", amount_col)] if col is None]
        if missing:
            raise ValueError(
                f"Could not detect columns: {missing}. Available columns: {cols}"
            )

        transactions: list[Transaction] = []
        for _, row in df.iterrows():
            raw_date = str(row[date_col]).strip()
            raw_amount = str(row[amount_col]).strip()
            raw_desc = str(row[desc_col]).strip() if desc_col else ""

            if not raw_date or raw_date == "nan" or not raw_amount or raw_amount == "nan":
                continue

            try:
                tx_date = date.fromisoformat(raw_date[:10])
            except ValueError:
                continue

            try:
                amount = -_parse_amount(raw_amount)
            except ValueError:
                continue

            memo = raw_desc if raw_desc and raw_desc != "nan" else ""
            import_id = Transaction.make_import_id(BANK, tx_date, amount, memo)
            transactions.append(Transaction(
                date=tx_date,
                amount_milliunits=amount,
                payee=memo or f"Spendwise {tx_date}",
                memo=memo,
                import_id=import_id,
            ))

        return transactions
