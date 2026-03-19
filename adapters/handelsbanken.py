import re
from datetime import date
from pathlib import Path

import pandas as pd

from adapters.base import BaseAdapter, find_col, iter_rows, load_df, parse_amount
from models import Transaction

BANK = "handelsbanken"

_DATE_PAT = re.compile(r"\b(transaktionsdatum|datum)\b", re.I)
_AMOUNT_PAT = re.compile(r"\bbelopp\b", re.I)
_DESC_PAT = re.compile(r"\btext\b", re.I)


_SALDO_PAT = re.compile(r"Saldo:\s*([\d\s,.-]+)", re.I)


def parse_balance(filepath: Path) -> int | None:
    """Extract the closing balance from the Handelsbanken export header → milliunits, or None."""
    raw = pd.read_excel(filepath, header=None, dtype=str, nrows=10)
    for _, row in raw.iterrows():
        for cell in row:
            if pd.isna(cell):
                continue
            m = _SALDO_PAT.search(str(cell))
            if m:
                return parse_amount(m.group(1).strip())
    return None


class HandelsbankenAdapter(BaseAdapter):
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
                amount = parse_amount(raw_amount)
            except ValueError:
                continue

            memo = raw_desc or ""
            import_id = Transaction.make_import_id(BANK, tx_date, amount, memo)
            transactions.append(Transaction(
                date=tx_date,
                amount_milliunits=amount,
                payee=memo or f"Handelsbanken {tx_date}",
                memo=memo,
                import_id=import_id,
            ))

        return transactions
