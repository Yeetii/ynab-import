from dataclasses import dataclass
from datetime import date
from typing import Literal
from zlib import crc32


@dataclass
class Transaction:
    date: date
    amount_milliunits: int  # negative = outflow (YNAB convention)
    payee: str
    memo: str
    import_id: str          # deterministic: f"{bank}:{date}:{amount}:{crc32(memo)}"

    @staticmethod
    def make_import_id(bank: str, tx_date: date, amount_milliunits: int, memo: str) -> str:
        checksum = crc32(memo.encode()) & 0xFFFFFFFF
        return f"{bank}:{tx_date}:{amount_milliunits}:{checksum}"


@dataclass
class MatchResult:
    bank_tx: Transaction
    ynab_tx: dict | None    # None = no match found (create new)
    confidence: float       # 0.0–1.0
    action: Literal["match", "create", "skip"]  # skip = already imported
