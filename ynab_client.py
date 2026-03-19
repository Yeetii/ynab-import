from datetime import date

import requests

import config
from models import Transaction

BASE_URL = "https://api.ynab.com/v1"


class YnabClient:
    def __init__(self) -> None:
        self._session = requests.Session()
        self._session.headers["Authorization"] = f"Bearer {config.YNAB_API_TOKEN}"
        self._budget_id = config.YNAB_BUDGET_ID

    def _url(self, path: str) -> str:
        return f"{BASE_URL}/{path}"

    def _raise(self, resp: requests.Response) -> None:
        if not resp.ok:
            try:
                detail = resp.json().get("error", {}).get("detail", resp.text)
            except Exception:
                detail = resp.text
            raise RuntimeError(f"YNAB API {resp.status_code}: {detail}")

    def get_transactions(self, account_id: str, since_date: date) -> list[dict]:
        resp = self._session.get(
            self._url(f"budgets/{self._budget_id}/accounts/{account_id}/transactions"),
            params={"since_date": since_date.isoformat()},
        )
        self._raise(resp)
        return resp.json()["data"]["transactions"]

    def create_transactions(self, account_id: str, transactions: list[Transaction]) -> dict:
        """Bulk-create up to 1000 transactions."""
        payload = {
            "transactions": [
                {
                    "account_id": account_id,
                    "date": tx.date.isoformat(),
                    "amount": tx.amount_milliunits,
                    "payee_name": tx.payee[:200],  # YNAB limit
                    "memo": tx.memo[:200],
                    "cleared": "cleared",
                    "import_id": tx.import_id[:36],  # YNAB limit
                }
                for tx in transactions
            ]
        }
        resp = self._session.post(
            self._url(f"budgets/{self._budget_id}/transactions"),
            json=payload,
        )
        self._raise(resp)
        return resp.json()["data"]

    def match_transaction(self, ynab_tx_id: str, import_id: str) -> dict:
        """Link a manually-entered YNAB transaction to a bank import by setting import_id."""
        payload = {
            "transaction": {
                "import_id": import_id[:36],
                "cleared": "cleared",
            }
        }
        resp = self._session.patch(
            self._url(f"budgets/{self._budget_id}/transactions/{ynab_tx_id}"),
            json=payload,
        )
        self._raise(resp)
        return resp.json()["data"]["transaction"]
