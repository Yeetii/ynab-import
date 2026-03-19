import re
from datetime import date
from pathlib import Path

from adapters.base import BaseAdapter, find_col, iter_rows, load_df, parse_amount
from models import Transaction

BANK = "spendwise"

_DATE_PAT = re.compile(r"\b(date|datum)\b", re.I)
_AMOUNT_PAT = re.compile(r"\b(amount|belopp|sum)\b", re.I)
_DESC_PAT = re.compile(r"\b(description|text|payee|memo|kommentar|beskrivning|specifikation)\b", re.I)


class SpendwiseAdapter(BaseAdapter):
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
