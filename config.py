import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")


def _require(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f"Missing required env var: {key}")
    return val


YNAB_API_TOKEN = _require("YNAB_API_TOKEN")
YNAB_BUDGET_ID = _require("YNAB_BUDGET_ID")
YNAB_ACCOUNT_HANDELSBANKEN = os.getenv("YNAB_ACCOUNT_HANDELSBANKEN", "")
YNAB_ACCOUNT_SPENDWISE = os.getenv("YNAB_ACCOUNT_SPENDWISE", "")

ACCOUNT_IDS = {
    "handelsbanken": YNAB_ACCOUNT_HANDELSBANKEN,
    "spendwise": YNAB_ACCOUNT_SPENDWISE,
}
