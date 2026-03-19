import re
from datetime import date
from pathlib import Path

import pandas as pd

from adapters.base import BaseAdapter
from models import Transaction

BANK = "handelsbanken"


def _parse_swedish_amount(raw: str) -> int:
    """Parse Swedish number format like '-1 234,56' → milliunits int."""
    cleaned = re.sub(r"\s", "", str(raw))   # remove all whitespace (thousands sep)
    cleaned = cleaned.replace(",", ".")
    return round(float(cleaned) * 1000)


class HandelsbankenAdapter(BaseAdapter):
    def parse(self, filepath: Path) -> list[Transaction]:
        # Find the header row (first row containing "Datum")
        raw = pd.read_excel(filepath, header=None, dtype=str)
        header_row = None
        for i, row in raw.iterrows():
            if row.astype(str).str.strip().eq("Datum").any():
                header_row = i
                break

        if header_row is None:
            raise ValueError("Could not find 'Datum' header row in Handelsbanken export")

        df = pd.read_excel(filepath, header=header_row, dtype=str)
        df.columns = df.columns.str.strip()

        # Identify columns (flexible — some exports add extra cols)
        date_col = next(c for c in df.columns if c.strip() == "Datum")
        text_col = next(c for c in df.columns if c.strip() == "Text")
        amount_col = next(c for c in df.columns if c.strip() == "Belopp")

        transactions: list[Transaction] = []
        for _, row in df.iterrows():
            raw_date = str(row[date_col]).strip()
            raw_text = str(row[text_col]).strip()
            raw_amount = str(row[amount_col]).strip()

            # Skip blank / total rows
            if not raw_date or raw_date in ("nan", "Datum") or not raw_amount or raw_amount == "nan":
                continue

            try:
                tx_date = date.fromisoformat(raw_date[:10])
            except ValueError:
                continue

            try:
                amount = _parse_swedish_amount(raw_amount)
            except ValueError:
                continue

            import_id = Transaction.make_import_id(BANK, tx_date, amount, raw_text)
            transactions.append(Transaction(
                date=tx_date,
                amount_milliunits=amount,
                payee=raw_text,
                memo=raw_text,
                import_id=import_id,
            ))

        return transactions
