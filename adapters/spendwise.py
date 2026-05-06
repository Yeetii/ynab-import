import re
from datetime import date
from pathlib import Path

import pandas as pd

from adapters.base import BaseAdapter, find_col, iter_rows, load_df, parse_amount
from models import Transaction

BANK = "spendwise"

_DATE_PAT = re.compile(r"\b(date|datum)\b", re.I)
_AMOUNT_PAT = re.compile(r"\b(amount|belopp|sum)\b", re.I)
_DESC_PAT = re.compile(r"\b(description|text|payee|memo|kommentar|beskrivning|specifikation)\b", re.I)
_BOKFORT_PAT = re.compile(r"\bbokf.rt\b", re.I)
_BETALT_PAT = re.compile(r"\bBETALT\b", re.I)


def parse_balance(filepath: Path, reference_date: date | None = None) -> int | None:
    """Compute the current outstanding balance in milliunits.

    Sums all charges (by booking date) within the last 2 calendar months
    relative to *reference_date* (defaults to the latest booking date in the
    file), excluding monthly payment rows (BETALT BG).

    Returns None if the required columns cannot be detected.
    """
    df = load_df(filepath, _DATE_PAT)
    df.columns = [c.strip() for c in df.columns]
    cols = list(df.columns)

    bokfort_col = find_col(cols, _BOKFORT_PAT)
    amount_col = find_col(cols, _AMOUNT_PAT)
    desc_col = find_col(cols, _DESC_PAT)

    if bokfort_col is None or amount_col is None:
        return None

    valid = df[df[bokfort_col].str.match(r"\d{4}-\d{2}-\d{2}", na=False)].copy()
    if valid.empty:
        return None

    valid["_bokfort"] = pd.to_datetime(valid[bokfort_col].str[:10]).dt.date

    ref = reference_date or max(valid["_bokfort"])
    prev_month_start = date(ref.year - 1, 12, 1) if ref.month == 1 else date(ref.year, ref.month - 1, 1)

    window = valid[(valid["_bokfort"] >= prev_month_start) & (valid["_bokfort"] <= ref)]

    if desc_col:
        window = window[~window[desc_col].str.contains(_BETALT_PAT, na=False)]

    total = 0
    for raw in window[amount_col]:
        try:
            total += parse_amount(str(raw))
        except (ValueError, AttributeError):
            pass
    # Negate: charges are positive in the export but YNAB represents credit card
    # balances as negative (sum of outflows).
    return -total


class SpendwiseAdapter(BaseAdapter):
    @staticmethod
    def detect(filepath: Path) -> bool:
        """Return True if filepath looks like a Spendwise export."""
        try:
            raw = pd.read_excel(filepath, header=None, dtype=str, nrows=5)
            for _, row in raw.iterrows():
                for cell in row:
                    if pd.notna(cell) and re.search(r"Transaktionsexport", str(cell), re.I):
                        return True
        except Exception:
            pass
        return False

    def parse(self, filepath: Path) -> list[Transaction]:
        df = load_df(filepath, _DATE_PAT)
        df.columns = [c.strip() for c in df.columns]
        cols = list(df.columns)

        date_col = find_col(cols, _DATE_PAT)
        amount_col = find_col(cols, _AMOUNT_PAT)
        desc_col = find_col(cols, _DESC_PAT)

        missing = [name for name, col in [("date", date_col), ("amount", amount_col)] if col is None]
        if missing:
            raise ValueError(f"Could not detect columns: {missing}. Available: {cols}")

        transactions: list[Transaction] = []
        for raw_date, raw_amount, raw_desc in iter_rows(df, date_col, amount_col, desc_col):
            try:
                tx_date = date.fromisoformat(raw_date[:10])
            except ValueError:
                continue
            try:
                # Spendwise: charges are positive in export → negate for YNAB convention
                amount = -parse_amount(raw_amount)
            except ValueError:
                continue

            memo = raw_desc or ""
            import_id = Transaction.make_import_id(BANK, tx_date, amount, memo)
            transactions.append(Transaction(
                date=tx_date,
                amount_milliunits=amount,
                payee=memo or f"Spendwise {tx_date}",
                memo=memo,
                import_id=import_id,
            ))

        return transactions
